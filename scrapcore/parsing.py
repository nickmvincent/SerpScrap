# -*- coding: utf-8 -*-
import logging
import re

from scrapcore.database import SearchEngineResultsPage
from scrapcore.parser.google_parser import GoogleParser

logger = logging.getLogger(__name__)


class Parsing():
    """This class handles HTML parsing"""

    def get_parser_by_url(self, url):
        """Get the appropriate parser by an search engine url."""
        parser = None

        if re.search(r'^http[s]?://www\.google', url):
            parser = GoogleParser
        if not parser:
            raise Exception('No parser for {}.'.format(url))

        return parser

    def get_parser_by_search_engine(self, search_engine):
        """Get the appropriate parser for the search_engine"""
        if search_engine == 'google' or search_engine == 'googleimg':
            return GoogleParser
        else:
            raise Exception('No such parser for "{}"'.format(search_engine))

    def parse_serp(self,
                   config,
                   html=None,
                   parser=None,
                   scraper=None,
                   search_instance=None,
                   query='',):
        """parse and store data in the sqlalchemy session.
        Returns:
            The parsed SERP object.
        """

        if not parser and html:
            parser = self.get_parser_by_search_engine(search_instance['engine'])
            parser = parser(config, query=query)
            parser.parse(html)

        serp = SearchEngineResultsPage()

        if query:
            serp.query = query

        if parser:
            serp.set_values_from_parser(parser)
        if scraper:
            serp.set_values_from_scraper(scraper)
        serp.latitude = search_instance.get('latitude')
        serp.longitude = search_instance.get('longitude')
        serp.urban_rural_code = search_instance.get('urban_rural_code')
        serp.median_income = search_instance.get('median_income')
        serp.percent_dem = search_instance.get('percent_dem')
        serp.reported_location = scraper.reported_location

        return serp
