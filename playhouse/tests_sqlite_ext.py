import sqlite3
import unittest

from peewee import *
import sqlite_ext as sqe

# test aggregate.
class WeightedAverage(object):
    def __init__(self):
        self.total_weight = 0.0
        self.total_ct = 0.0

    def step(self, value, wt=None):
        wt = wt or 1.0
        self.total_weight += wt
        self.total_ct += wt * value

    def finalize(self):
        if self.total_weight != 0.0:
            return self.total_ct / self.total_weight
        return 0.0

# test collation
def collate_reverse(s1, s2):
    return -cmp(s1, s2)

# test function
def title_case(s):
    return s.title()

# use a disk-backed db since memory dbs only exist for a single connection and
# we need to share the db w/2 for the locking tests.  additionally, set the
# sqlite_busy_timeout to 100ms so when we test locking it doesn't take forever
ext_db = sqe.SqliteExtDatabase('tmp.db', timeout=.1)

# register test aggregates / collations / functions
ext_db.register_aggregate(WeightedAverage, 1, 'weighted_avg')
ext_db.register_aggregate(WeightedAverage, 2, 'weighted_avg2')
ext_db.register_collation(collate_reverse)
ext_db.register_function(title_case)


class BaseExtModel(sqe.Model):
    class Meta:
        database = ext_db

class Post(BaseExtModel):
    message = TextField()

class FTSPost(Post, sqe.FTSModel):
    pass

class Values(BaseExtModel):
    klass = IntegerField()
    value = FloatField()
    weight = FloatField()


class SqliteExtTestCase(unittest.TestCase):
    messages = [
        'A faith is a necessity to a man. Woe to him who believes in nothing.',
        'All who call on God in true faith, earnestly from the heart, will '
        'certainly be heard, and will receive what they have asked and desired.',
        'Be faithful in small things because it is in them that your strength lies.',
        'Faith consists in believing when it is beyond the power of reason to believe.',
        'Faith has to do with things that are not seen and hope with things that are not at hand.',
    ]
    def setUp(self):
        FTSPost.drop_table(True)
        Post.drop_table(True)
        Values.drop_table(True)
        Values.create_table()
        Post.create_table()
        FTSPost.create_table(tokenize='porter', content_model=Post)

    def test_fts(self):
        matches = lambda s: sqe.match(FTSPost.message, s)
        posts = []
        for message in self.messages:
            posts.append(Post.create(message=message))

        # Nothing matches, index is not built.
        pq = FTSPost.select().where(matches('faith'))
        self.assertEqual(list(pq), [])

        FTSPost.rebuild()
        FTSPost.optimize()

        # it will stem faithful -> faith b/c we use the porter tokenizer
        pq = FTSPost.select().where(matches('faith')).order_by('id')
        self.assertEqual([x.message for x in pq], self.messages)

        pq = FTSPost.select().where(matches('believe')).order_by('id')
        self.assertEqual([x.message for x in pq], [
            self.messages[0],
            self.messages[3],
        ])

        pq = FTSPost.select().where(matches('thin*')).order_by('id')
        self.assertEqual([x.message for x in pq], [
            self.messages[2],
            self.messages[4],
        ])

        pq = FTSPost.select().where(matches('"it is"')).order_by('id')
        self.assertEqual([x.message for x in pq], [
            self.messages[2],
            self.messages[3],
        ])

        pq = (FTSPost
              .select(FTSPost, sqe.Rank(FTSPost).alias('score'))
              .where(matches('things'))
              .order_by(R('score').desc()))
        self.assertEqual([(x.message, x.score) for x in pq], [
            (self.messages[4], 2.0 / 3),
            (self.messages[2], 1.0 / 3),
        ])

        pq = FTSPost.select(sqe.Rank(FTSPost)).where(matches('faithful')).tuples()
        self.assertEqual([x[0] for x in pq], [.2] * 5)

    def test_custom_agg(self):
        data = (
            (1, 3.4, 1.0),
            (1, 6.4, 2.3),
            (1, 4.3, 0.9),
            (2, 3.4, 1.4),
            (3, 2.7, 1.1),
            (3, 2.5, 1.1),
        )
        for klass, value, wt in data:
            Values.create(klass=klass, value=value, weight=wt)

        vq = (Values
              .select(
                  Values.klass,
                  fn.weighted_avg(Values.value).alias('wtavg'),
                  fn.avg(Values.value).alias('avg'))
              .group_by(Values.klass))
        q_data = [(v.klass, v.wtavg, v.avg) for v in vq]
        self.assertEqual(q_data, [
            (1, 4.7, 4.7),
            (2, 3.4, 3.4),
            (3, 2.6, 2.6),
        ])

        vq = (Values
              .select(
                  Values.klass,
                  fn.weighted_avg2(Values.value, Values.weight).alias('wtavg'),
                  fn.avg(Values.value).alias('avg'))
              .group_by(Values.klass))
        q_data = [(v.klass, str(v.wtavg)[:4], v.avg) for v in vq]
        self.assertEqual(q_data, [
            (1, '5.23', 4.7),
            (2, '3.4', 3.4),
            (3, '2.6', 2.6),
        ])

    def test_custom_collation(self):
        for i in [1, 4, 3, 5, 2]:
            Post.create(message='p%d' % i)

        pq = Post.select().order_by(Clause(Post.message, R('collate collate_reverse')))
        self.assertEqual([p.message for p in pq], ['p5', 'p4', 'p3', 'p2', 'p1'])

    def test_custom_function(self):
        p1 = Post.create(message='this is a test')
        p2 = Post.create(message='another TEST')

        sq = Post.select().where(fn.title_case(Post.message) == 'This Is A Test')
        self.assertEqual(list(sq), [p1])

        sq = Post.select(fn.title_case(Post.message)).tuples()
        self.assertEqual([x[0] for x in sq], [
            'This Is A Test',
            'Another Test',
        ])

    def test_granular_transaction(self):
        conn = ext_db.get_conn()

        def test_locked_dbw(lt):
            with ext_db.granular_transaction(lt):
                Post.create(message='p1')  # Will not be saved.
                conn2 = ext_db._connect(ext_db.database, **ext_db.connect_kwargs)
                conn2.execute('insert into post (message) values (?);', ('x1',))
        self.assertRaises(sqlite3.OperationalError, test_locked_dbw, 'exclusive')
        self.assertRaises(sqlite3.OperationalError, test_locked_dbw, 'immediate')
        self.assertRaises(sqlite3.OperationalError, test_locked_dbw, 'deferred')

        def test_locked_dbr(lt):
            with ext_db.granular_transaction(lt):
                Post.create(message='p2')
                conn2 = ext_db._connect(ext_db.database, **ext_db.connect_kwargs)
                res = conn2.execute('select message from post')
                return res.fetchall()

        # no read-only stuff with exclusive locks
        self.assertRaises(sqlite3.OperationalError, test_locked_dbr, 'exclusive')

        # ok to do readonly w/immediate and deferred (p2 is saved twice)
        self.assertEqual(test_locked_dbr('immediate'), [])
        self.assertEqual(test_locked_dbr('deferred'), [('p2',)])

        # test everything by hand, by setting the default connection to
        # 'exclusive' and turning off autocommit behavior
        ext_db.set_autocommit(False)
        conn.isolation_level = 'exclusive'
        Post.create(message='p3')  # uncommitted

        # now, open a second connection w/exclusive and try to read, it will
        # be locked
        conn2 = ext_db._connect(ext_db.database, **ext_db.connect_kwargs)
        conn2.isolation_level = 'exclusive'
        self.assertRaises(sqlite3.OperationalError, conn2.execute, 'select * from post')

        # rollback the first connection's transaction, releasing the exclusive lock
        conn.rollback()
        ext_db.set_autocommit(True)

        with ext_db.granular_transaction('deferred'):
            Post.create(message='p4')

        res = conn2.execute('select message from post order by message;')
        self.assertEqual([x[0] for x in res.fetchall()], [
            'p2', 'p2', 'p4'])
