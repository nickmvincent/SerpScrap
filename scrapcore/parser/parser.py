# -*- coding: utf-8 -*-
import logging
import pprint
import re

from cssselect import HTMLTranslator
import lxml.html
from lxml.html.clean import Cleaner


logger = logging.getLogger(__name__)


class Parser():
    """Default Parse"""

    no_results_selector = []
    effective_query_selector = []
    num_results_search_selectors = []
    page_number_selectors = []
    search_types = []

    def __init__(self, config={}, html='', query=''):
        """Create new Parser instance and parse all information."""
        self.config = config
        self.searchtype = self.config.get('search_type', 'normal')
        assert self.searchtype in self.search_types, 'search type "{}" is not supported in {}'.format(
            self.searchtype,
            self.__class__.__name__
        )

        self.query = query
        self.html = html
        self.dom = None
        self.search_results = {}
        self.num_results_for_query = ''
        self.num_results = 0
        self.effective_query = ''
        self.page_number = -1
        self.no_results = False
        self.related_keywords = {}

        # to be set by the implementing sub classes
        self.search_engine = ''

        # short alias because we use it so extensively
        self.css_to_xpath = HTMLTranslator().css_to_xpath

        if self.html:
            self.parse()

    def parse(self, html=None):
        """Public function to start parsing the search engine results.

        Args:
            html: The raw html data to extract the SERP entries from.
        """
        if html:
            self.html = html.encode('utf-8').decode('utf-8')

        # lets do the actual parsing
        self._parse()

        # Apply subclass specific behaviour after parsing has happened
        # This is needed because different parsers need to clean/modify
        # the parsed data uniquely.
        self.after_parsing()

    def _parse_lxml(self, cleaner=None):
        try:
            parser = lxml.html.HTMLParser(encoding='utf-8')
            if cleaner:
                self.dom = cleaner.clean_html(self.dom)
            self.dom = lxml.html.document_fromstring(self.html, parser=parser)
            self.dom.resolve_base_href()
        except Exception as e:
            # maybe wrong encoding
            logger.error(e)

    def _parse(self, cleaner=None):
        """Internal parse the dom according to the provided css selectors.
        Raises: Exception
        if no css selectors for the searchtype could be found.
        """
        self.num_results = 0
        self._parse_lxml(cleaner)

        # try to parse the number of results.
        attr_name = self.searchtype + '_search_selectors'
        selector_dict = getattr(self, attr_name, None)

        # get the appropriate css selectors for the num_results for the keyword
        num_results_selector = getattr(
            self,
            'num_results_search_selectors',
            None
        )

        self.num_results_for_query = self.first_match(
            num_results_selector,
            self.dom
        )
        if not self.num_results_for_query:
            logger.debug(''''{}: Cannot parse num_results from serp page
            with selectors {}
            '''.format(self.__class__.__name__, num_results_selector))

        # get the current page we are at.
        try:
            self.page_number = int(
                self.first_match(self.page_number_selectors, self.dom)
            )
        except ValueError:
            self.page_number = -1

        # let's see if the search query was shitty (no results for that query)
        self.effective_query = self.first_match(
            self.effective_query_selector,
            self.dom
        )
        if self.effective_query:
            logger.debug('''{}: There was no search hit for the search query.
            Search engine used {} instead.
            '''.format(self.__class__.__name__, self.effective_query))
        else:
            self.effective_query = ''

        # the element that notifies the user about no results.
        self.no_results_text = self.first_match(
            self.no_results_selector,
            self.dom
        )

        # get the stuff that is of interest in SERP pages.
        if not selector_dict and not isinstance(selector_dict, dict):
            raise Exception('''There is no such attribute: {}. No selectors found
            '''.format(attr_name))

        for result_type, selector_class in selector_dict.items():
            # might be "key:ads_main, value:{us_ip: {...}, de_ip: {...}}"
            # for example, one iteration of this loop would handle all the "ads_main" items
            self.search_results[result_type] = []
            self.related_keywords[result_type] = []

            for _, selectors in selector_class.items():
                # each key will be "us_ip, de_ip, etc"
                # each value (selectors) is yet another dict, the key is name of selector (e.g. "container")
                # and the values in "selectors" is the actual css selector (e.g. "#center_col")

                # this means the us_ip selectors AND the de_ip selectors will be used, but duplicates are not logged
                if 'result_container' in selectors and selectors['result_container']:
                    css = '{container} {result_container}'.format(**selectors)
                else:
                    css = selectors['container']
                results = self.dom.xpath(
                    self.css_to_xpath(css)
                )

                to_extract = set(selectors.keys()) - {'container', 'result_container'}
                selectors_to_use = {key: selectors[key] for key in to_extract if key in selectors.keys()}

                # if you skip an item for a real reason (e.g. prerender links)
                # then increment num_rightfully_skipped to avoid messing up rank calcs
                num_rightfully_skipped = 0

                for index, result in enumerate(results):
                    # Let's add primitive support for CSS3 pseudo selectors
                    serp_result = {}
                    # key are for example 'link', 'snippet', 'visible-url', ...
                    # selector is the selector to grab these items
                    for key, selector in selectors_to_use.items():
                        serp_result[key] = self.advanced_css(selector, result)

                    # # skip prerender links
                    # has_prerender = self.advanced_css('link::attr(rel)', result)
                    # if has_prerender == 'prerender':
                    #     num_rightfully_skipped += 1
                    #     continue
                        

                    # only add items that have not None links.
                    # Avoid duplicates. Detect them by the link.
                    # If statement below: Lazy evaluation.
                    # The more probable case first.
                    found_container = False
                    serp_result['rank'] = index + 1 - num_rightfully_skipped
                    for key in ['isTweetCarousel', 'isMapsPlaces', 'isMapsLocations', 'isNewsCarousel', 'isKnowledgeBox']:
                        if serp_result.get(key):
                            serp_result[key] = True
                            found_container = True
                    if serp_result.get('isKnowledgeBox'):
                        all_content = self.advanced_css('*', result)
                        serp_result['misc'] = all_content
                    if (
                        found_container
                    ) or (
                        'link' in serp_result and serp_result['link'] and
                            not [e for e in self.search_results[result_type]
                                 if e['link'] == serp_result['link'] and not e.get('isKnowledgeBox')]
                    ) or (
                        result_type in [
                            'knowledge_panel', 'tweets',
                            'maps_places', 'maps_locations',
                        ] or serp_result.get('isKnowledgeBox')
                    ):
                        self.search_results[result_type].append(serp_result)
                        self.num_results += 1
                    elif 'keyword' in serp_result and serp_result['keyword']:
                        self.related_keywords[result_type].append(serp_result)
                    else:
                        num_rightfully_skipped += 1



    def advanced_css(self, selector, element):
        """Evaluate the :text and ::attr(attr-name) additionally.

        Args:
            selector: A css selector.
            element: The element on which to apply the selector.

        Returns:
            The targeted element.

        """
        value = None

        if selector.endswith('::text'):
            try:
                value = element.xpath(self.css_to_xpath(selector.split('::')[0]))[0].text_content()
            except IndexError:
                pass
        else:
            match = re.search(r'::attr\((?P<attr>.*)\)$', selector)

            if match:
                attr = match.group('attr')
                try:
                    value = element.xpath(self.css_to_xpath(selector.split('::')[0]))[0].get(attr)
                except IndexError:
                    pass
            else:
                try:
                    value = element.xpath(self.css_to_xpath(selector))[0].text_content()
                except IndexError:
                    pass

        return value

    def first_match(self, selectors, element):
        """Get the first match.

        Args:
            selectors: The selectors to test for a match.
            element: The element on which to apply the selectors.

        Returns:
            The very first match or False if all selectors didn't match anything.
        """
        assert isinstance(selectors, list), 'selectors must be of type list!'

        for selector in selectors:
            if selector:
                try:
                    match = self.advanced_css(selector, element=element)
                    if match:
                        return match
                except IndexError:
                    pass
        return False
    def after_parsing(self):
        """Subclass specific behaviour after parsing happened.

        Override in subclass to add search engine specific behaviour.
        Commonly used to clean the results.
        """

    def __str__(self):
        """Return a nicely formatted overview of the results."""
        return pprint.pformat(self.search_results)

    @property
    def cleaned_html(self):
        """Try to parse the provided HTML string using lxml
        strip all unnecessary information to save space"""
        cleaner = Cleaner()
        cleaner.scripts = True
        cleaner.javascript = True
        cleaner.comments = True
        cleaner.style = True
        self.dom = cleaner.clean_html(self.dom)
        assert self.dom, 'The html needs to be parsed to get the cleaned html'
        return lxml.html.tostring(self.dom)

    def iter_serp_items(self):
        """Yields the key and index of any item in the serp results that has a link value"""

        for key, value in self.search_results.items():
            if isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict) and item['link']:
                        yield (key, i)

