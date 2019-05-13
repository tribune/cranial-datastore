"""
Testing for db/util/redshift.py
"""
import unittest
from cranial.db.utils.redshift import execute_redshift_query
from unittest import mock

class TestRedshiftUtil(unittest.TestCase):
    TEST_KEY_PATH = '/local/key/file'
    TEST_EXEC_SQL = 'SELECT * FROM table LIMIT 5;'
    @mock.patch("psycopg2.connect")
    @mock.patch("cranial.db.utils.redshift._get_redshift_connection")
    def test_execute_redshift_query(self, mock_connect, mock_open):
        # This is the result of psycopg2.connect()
        mock_con = mock_connect.return_value  
        mock_open.return_value = mock_con
        # This is the result of connection.cursor()
        mock_cur = mock_con.cursor.return_value.__enter__.return_value
        execute_redshift_query(TestRedshiftUtil.TEST_EXEC_SQL,
                               TestRedshiftUtil.TEST_KEY_PATH,
                               False)
        mock_cur.execute.assert_called_once_with(TestRedshiftUtil.TEST_EXEC_SQL)

    @mock.patch("psycopg2.connect")
    @mock.patch("cranial.db.utils.redshift._get_redshift_connection")
    def test_dryrun_redshift_query(self, mock_connect, mock_open):
        mock_con = mock_connect.return_value  
        mock_open.return_value = mock_con
        mock_cur = mock_con.cursor.return_value.__enter__.return_value
        execute_redshift_query(TestRedshiftUtil.TEST_EXEC_SQL,
                               TestRedshiftUtil.TEST_KEY_PATH,
                               True)
        mock_cur.execute.assert_not_called()