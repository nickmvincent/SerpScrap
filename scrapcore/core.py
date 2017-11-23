"""runs serpscrap"""
#!/usr/bin/python3
# -*- coding: utf-8 -*-
import datetime
import queue
import threading
import time

from random import shuffle
from scrapcore.cachemanager import CacheManager
from scrapcore.database import ScraperSearch
from scrapcore.database import get_session, fixtures
from scrapcore.logger import Logger
from scrapcore.result_writer import ResultWriter
from scrapcore.scraper.scrape_worker_factory import ScrapeWorkerFactory
from scrapcore.tools import Proxies
from scrapcore.tools import ScrapeJobGenerator
from scrapcore.tools import ShowProgressQueue
from scrapcore.validator_config import ValidatorConfig

USE_CONTROL = True

class Core():
    """The core object runs all other code"""
    logger = None

    def run(self, config):
        """run with the dict in config."""
        validator = ValidatorConfig()
        validator.validate(config)

        return self.main(return_results=True, config=config)

    def main(self, return_results=False, config=None):
        """the main method"""

        logger = Logger()
        logger.setup_logger(level=config.get('log_level').upper())
        self.logger = logger.get_logger()

        proxy_file = config.get('proxy_file', '')

        search_instances = config.get('search_instances', [{'engine':'google'}])
        if not isinstance(search_instances, list):
            raise ValueError('Please provide a list of search instance objects')

        num_search_instances = len(search_instances)
        num_workers = int(config.get('num_workers'))
        scrape_method = config.get('scrape_method')
        pages = int(config.get('num_pages_for_keyword', 1))
        method = config.get('scrape_method', 'selenium')

        all_keyword_objs = set(config.get('keywords', []))
        scraper_searches = []
        for index, keyword_obj in enumerate(all_keyword_objs):
            keywords = [keyword_obj['keyword']]
            category = keyword_obj['category']

            result_writer = ResultWriter()
            result_writer.init_outfile(config, force_reload=True)
            cache_manager = CacheManager(config, self.logger, result_writer)

            scrape_jobs = ScrapeJobGenerator().get(
                keywords,
                search_instances,
                scrape_method,
                pages
            )
            scrape_jobs = list(scrape_jobs)

            if USE_CONTROL:
                control_jobs = ScrapeJobGenerator().get(
                    keywords,
                    search_instances,
                    scrape_method,
                    pages
                )
            else:
                control_jobs = []
            control_jobs = list(control_jobs)
            proxies = []

            if config.get('use_own_ip'):
                proxies.append(None)
            elif proxy_file:
                proxies = Proxies().parse_proxy_file(proxy_file)

            if not proxies:
                raise Exception('''No proxies available. Turning down.''')
            shuffle(proxies)

            # get a scoped sqlalchemy session
            session_cls = get_session(config, scoped=True)
            session = session_cls()

            # add fixtures
            fixtures(config, session)

            # add proxies to the database
            Proxies().add_proxies_to_db(proxies, session)

            scraper_search = ScraperSearch(
                number_search_instances_used=num_search_instances,
                number_proxies_used=len(proxies),
                number_search_queries=len(keywords),
                started_searching=datetime.datetime.utcnow(),
                used_search_instances=','.join(
                    [instance['engine'] for instance in search_instances]
                )
            )

            # first check cache
            if config.get('do_caching'):
                scrape_jobs = cache_manager.filter_scrape_jobs(
                    scrape_jobs,
                    session,
                    scraper_search
                )
            if scrape_jobs:
                # Create a lock to synchronize database
                # access in the sqlalchemy session
                db_lock = threading.Lock()

                # create a lock to cache results
                cache_lock = threading.Lock()

                # A lock to prevent multiple threads from solving captcha,
                # used in selenium instances.
                captcha_lock = threading.Lock()

                self.logger.info(
                    '''
                    Going to scrape {num_keywords} keywords with {num_proxies}
                    proxies by using {num_threads} threads.
                    '''.format(
                        num_keywords=len(scrape_jobs),
                        num_proxies=len(proxies),
                        num_threads=num_search_instances)
                    )

                progress_thread = None

                # Show the progress of the scraping
                q = queue.Queue()
                progress_thread = ShowProgressQueue(config, q, len(scrape_jobs))
                progress_thread.start()

                workers = queue.Queue()
                control_workers = queue.Queue()
                num_worker = 0

                for _, search_instance in enumerate(search_instances):
                    for proxy in proxies:
                        for worker in range(num_workers):
                            num_worker += 1
                            workers.put(
                                ScrapeWorkerFactory(
                                    config,
                                    cache_manager=cache_manager,
                                    mode=method,
                                    proxy=proxy,
                                    search_instance=search_instance,
                                    session=session,
                                    db_lock=db_lock,
                                    cache_lock=cache_lock,
                                    scraper_search=scraper_search,
                                    captcha_lock=captcha_lock,
                                    progress_queue=q,
                                    browser_num=num_worker
                                )
                            )
                            if USE_CONTROL:
                                control_workers.put(
                                    ScrapeWorkerFactory(
                                        config,
                                        cache_manager=cache_manager,
                                        mode=method,
                                        proxy=proxy,
                                        search_instance=search_instance,
                                        session=session,
                                        db_lock=db_lock,
                                        cache_lock=cache_lock,
                                        scraper_search=scraper_search,
                                        captcha_lock=captcha_lock,
                                        progress_queue=q,
                                        browser_num=num_worker
                                    )
                                )

                # here we look for suitable workers
                # for all jobs created.
                for (joblist, workerq) in [
                        (scrape_jobs, workers),
                        (control_jobs, control_workers)
                ]:
                    for job in joblist:
                        while True:
                            worker = workerq.get()
                            workerq.put(worker)
                            if worker.is_suitable(job):
                                worker.add_job(job)
                                break

                threads, control_threads = [], []
                for (threadlist, workerq) in [
                        (threads, workers),
                        (control_threads, control_workers)
                ]:
                    while not workerq.empty():
                        worker = workerq.get()
                        thread = worker.get_worker()
                        if thread:
                            threadlist.append(thread)

                if len(threads) != len(control_threads) and USE_CONTROL:
                    q.put('done')
                    progress_thread.join()
                    raise ValueError("Something went wrong w/ threads, check config")

                if USE_CONTROL:
                    for thread, control_thread in zip(threads, control_threads):
                        thread.start()
                        thread.mark_category(category)
                        control_thread.mark_as_control()
                        control_thread.start()
                        control_thread.mark_category(category)
                else:
                    for thread in threads:
                        thread.start()
                for thread in threads:
                    thread.join()
                for thread in control_threads:
                    thread.join()

                # after threads are done, stop the progress queue.
                q.put('done')
                progress_thread.join()

            result_writer.close_outfile()

            scraper_search.stopped_searching = datetime.datetime.utcnow()
            try:
                session.add(scraper_search)
                session.commit()
            except Exception as err:
                print(err)
            scraper_searches.append(scraper_search)
            print('Finished with the keyword {}'.format(str(keywords)))
            if index != len(all_keyword_objs) - 1:
                sleep_mins = len(threads) + len(control_threads)
                print(
                    """
                    Going to sleep 1 minute per query made, for a total of {} minutes
                    """.format(sleep_mins)
                )
                time.sleep(60 * sleep_mins)

        if return_results:
            return scraper_searches
