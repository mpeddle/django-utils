from django.core.management.color import color_style, no_style
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models.loading import get_apps
from django.db import connection
from django.core.management.sql import sql_indexes

import re
from optparse import make_option

INSTALLED = settings.INSTALLED_APPS
index_re = re.compile('CREATE INDEX \`(.*?)\` ON \`(.*?)\` \(\`(.*?)\`')

#Gets you the current tables in MySQL
CURRENT_INDEX_SQL = """
SELECT
       column_name AS `Index`,
       table_name AS `Table`
FROM information_schema.statistics
WHERE table_schema = '{}'
GROUP BY 1,2;
"""
DB_SCHEMA = settings.DATABASES['default']['NAME']

class Command(BaseCommand):
    help = "Find the missing indexes in the database that Django thinks we should have."
    output_transaction = True
    option_list = BaseCommand.option_list + (
        make_option('--show', dest='show', default=False,
                    action="store_true", help='Show Changes'),
    )

    def handle(self, *args, **options):
        proposed_indexes, index_sql = self.proposed_indexes()
        indexes = self.indexes()

        #For all the proposed indexes, see if they exist
        #If not, tell us!
        for prop_name, prop_tables in proposed_indexes.items():
            for table in prop_tables:
                #find missing indexes
                try:
                    if table not in indexes[prop_name]:
                        if not options['show']:
                            print "(%s, %s) is missing from the DB" % (prop_name, table)
                        else:
                            for index in index_sql[table]:
                                if prop_name in index:
                                    print index
                #?? think through this logic more
                except KeyError:
                    if not options['show']:
                        print "No Indexes for %s in original db" % prop_name
                    else:
                        for index in index_sql[table]:
                            if table in index:
                                print index
    def proposed_indexes(self):
        """
        Return all indexes Django proposes & SQL for indexes
        """
        all_indexes = []
        proposed_indexes = {}
        index_sql = {}
        for app in get_apps():
            all_indexes.append(u'\n'.join(sql_indexes(app,
                                                      no_style(),connection)).encode('utf-8'))
        #Sort out all the proposed indexes by table.
        for index in all_indexes:
            indice = index.split('\n')
            for ind in indice:
                try:
                    match = index_re.search(ind)
                    name, table, field = match.groups()
                    if proposed_indexes.has_key(table):
                        proposed_indexes[table].append(field)
                    else:
                        proposed_indexes[table] = [field]
                    if index_sql.has_key(name):
                        index_sql[name].append(ind)
                    else:
                        index_sql[name] = [ind]
                except:
                    pass
        return proposed_indexes, index_sql
    def indexes(self):
        """
        Return all indexes in the DB
        """
        indexes = {}
        cursor = connection.cursor()
        vals = cursor.execute(CURRENT_INDEX_SQL.format(DB_SCHEMA))
        sql_back = cursor.fetchall()
        for row in sql_back:
            name, table = row
            if indexes.has_key(table):
                indexes[table].append(name)
            else:
                indexes[table] = [name]
        return indexes
