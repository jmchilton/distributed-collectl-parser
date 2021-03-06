#!/usr/bin/env python
import sys
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta
import time
import threading
import getpass
import Queue

import yaml

from cuisine import file_is_dir, mode_user, mode_local
from cuisine import run as c_run, cd

from fabric.api import run, get, put, execute, local, env
from fabric.tasks import Task
from fabric.context_managers import settings
# parse_collectl.py  --directory /project/collectl --hosts itasca,koronis,elmo,calhoun --num_threads 4

# regexp to match executables we don't care about and which shouldn't be recorded (mostly system tools).
BLACKLIST = "^(sshd|sshd:|/bin/.*|python|sh|perl|-?bash|cat|csh|.*/a.out|a.out|/usr/bin/ssh|ssh|/usr/bin/time|xargs|orted|mpirun|cp|pbs_demux|/opt/torque/.*|/opt/platform_mpi/.*|rm|.*workerbee.*|/usr/bin/python|touch|env|date|/usr/bin/perl|sleep|grep|/opt/openmpi/.*|.*/modulecmd|tee|gzip|tar|vi|make|/usr/bin/sh|less|/usr/bin/make|mv|pico|vim|scp|tail|sed|top|rsh|head|rsync|wc|awk|man|find)$"
MERGE_LIST = "^(.*g09.*|l\d+\.exel?)$"


def merge_executable(executable):
    return re.match(MERGE_LIST, executable) is not None


def filter_executable(executable):
    return re.match(BLACKLIST, executable) is not None


def filter_uid(uid):
    return not re.match("^\d+$", uid)


def to_postgres_date(time_object):
    """
    >>> time_tuple = CollectlSummary.parse_timestamp('20110831 15:29:59')
    >>> to_postgres_date(time_tuple)
    '2011-08-31 15:29:59'
    >>> to_postgres_date(datetime(*time_tuple[0:6]))
    '2011-08-31 15:29:59'
    """
    time_tuple = time_object
    if isinstance(time_object, datetime):
        time_tuple = time_object.timetuple()
    return time.strftime("%Y-%m-%d %H:%M:%S", time_tuple)


def escape_quotes(str):
    """
    >>> escape_quotes("'")
    "''"
    """
    return str.replace('\'', '\'\'')


class CollectlCommandLineBuilder:

    def __init__(self, collectl_path=None):
        if not collectl_path:
            self.collectl_path = "collectl"
        else:
            self.collectl_path = collectl_path

    def get(self, rawp_path):
        """
        Abstract this out of CollectlExecutor to allow for the unit
        testing of CollectlExecutor without collectl being installed via
        mocking this class.

        >>> builder = CollectlCommandLineBuilder()
        >>> builder.get("/project/collectl/itsaca/node0506-20110819-000100.rawp.gz")
        'collectl -sZ -P --sep=9 -p /project/collectl/itsaca/node0506-20110819-000100.rawp.gz'
        """
        return "%s -sZ -P --sep=9 -p %s" % (self.collectl_path, rawp_path)


class TestCommandLineBuilder:

    def get(self, rawp_path):
        return """echo Hello World """


class DateCutoffType:
    none, both, start, end = range(4)


class FabricCollectlExecutorFactory(Task):
    """
    """

    def __init__(self, host, user, collectl_path=None):
        self.exec_args = {"host": host}
        if user:
            self.exec_args["host"] = "%s@%s" % (user, host)
        self.collectl_path = collectl_path
        print "Instantiating Executor Factory"
        execute(self, **self.exec_args)

    def get_collectl_executor(self, rawp_file, stderr_file=None):
        return FabricCollectlExecutor(self.exec_args, rawp_file, stderr_file, "/tmp/collectl/collectl.pl")

    def run(self):
        print self.collectl_path
        put(os.path.dirname(self.collectl_path), "/tmp/")
        run("chmod +x /tmp/collectl/collectl.pl")


class FabricCollectlExecutor(Task):
    """
    """

    def __init__(self, exec_args, rawp_file, stderr_file=None, collectl_path=None):
        self.exec_args = exec_args
        self.rawp_file = rawp_file
        self.stderr_file = stderr_file
        self.stderr_temp = stderr_file is None

        stdout = tempfile.mkstemp()
        os.close(stdout[0])
        self.collectl_output_file = stdout[1]
        self.collectl_path = collectl_path

    def execute_collectl(self):
        execute(self, **self.exec_args)

    def run(self):
        if self.stderr_temp:
            stderr_tuple = tempfile.mkstemp()
            os.close(stderr_tuple[0])
            self.stderr_file = stderr_tuple[1]

        # We will manually check return code
        remote_stdout = run("mktemp /tmp/collectoutput.XXXXXXXXXX").strip()
        remote_stderr = run("mktemp /tmp/collecterror.XXXXXXXXXX").strip()
        remote_rawp = "/tmp/%s" % os.path.basename(self.rawp_file)
        put(self.rawp_file, remote_rawp)

        with settings(warn_only=True):
            collectl_command_line_builder = CollectlCommandLineBuilder(self.collectl_path)
            command_line = collectl_command_line_builder.get(remote_rawp)
            command_output = run("%s > %s 2> %s" % (command_line, remote_stdout, remote_stderr))
            return_code = command_output.return_code

        local("rm %s" % self.collectl_output_file)
        local("rm %s" % self.stderr_file)
        get(remote_stdout, self.collectl_output_file)
        get(remote_stderr, self.stderr_file)

        if return_code != 0:
            stderr_contents = self.__read_stderr()
            raise RuntimeError("collectl did not return a status code of 0, process standard error was %s" % stderr_contents)

        if self.stderr_temp:
            os.remove(self.stderr_file)

    def __read_stderr(self):
        file = open(self.stderr_file, 'r')
        try:
            contents = file.read()
            return contents
        finally:
            file.close()

    def output_file(self):
        return self.collectl_output_file

    def remove_output_file(self):
        os.remove(self.collectl_output_file)


class LocalCollectlExecutorFactory:
    """
    >>> import tempfile
    >>> stderr_file = tempfile.mkstemp()
    >>> executor_factory = LocalCollectlExecutorFactory()
    >>> executor = executor_factory.execute_collectl("/project/collectl/itsaca/node0506-20110819-000100.rawp.gz", stderr_file[1])
    >>> executor.collectl_command_line_builder = TestCommandLineBuilder()
    >>> executor.execute_collectl()
    >>> contents = open(executor.output_file(), "r").read()
    >>> contents.strip()
    'Hello World'
    """

    def __init__(self, host, user, collectl_path=None):
        self.collectl_path = collectl_path

    def get_collectl_executor(self, rawp_file, stderr_file=None):
        return CollectlExecutor(rawp_file, stderr_file, self.collectl_path)


class CollectlExecutor:

    def __init__(self, rawp_file, stderr_file=None, collectl_path=None):
        if not os.path.exists(rawp_file):
            temp_rawp = "/tmp/%s" % os.path.basename(rawp_file)
            get(rawp_file, temp_rawp)
            self.rawp_file = temp_rawp
            self.delete_rawp = True
        else:
            self.rawp_file = rawp_file
            self.delete_rawp = False

        self.stderr_file = stderr_file
        self.stderr_temp = stderr_file is None

        self.collectl_output_file = tempfile.mkstemp()
        self.collectl_command_line_builder = CollectlCommandLineBuilder(collectl_path)

    def execute_collectl(self):
        try:
            self._setup()
            self._run_collectl()
        finally:
            self._cleanup()

    def _setup(self):
        if self.stderr_temp:
            stderr_tuple = tempfile.mkstemp()
            os.close(stderr_tuple[0])
            self.stderr_file = stderr_tuple[1]

    def _cleanup(self):
        try:
            if self.delete_rawp:
                os.remove(self.rawp_file)
        finally:
            pass
        try:
            if self.stderr_temp:
                os.remove(self.stderr_file)
        finally:
            pass

    def _run_collectl(self):
        command_line = self.collectl_command_line_builder.get(self.rawp_file)
        stdout_fileno = self.collectl_output_file[0]
        stderr_stream = open(self.stderr_file, 'w')
        try:
            proc = subprocess.Popen(command_line, shell=True, stdout=stdout_fileno, stderr=stderr_stream)
            return_code = proc.wait()
        finally:
            os.close(stdout_fileno)
            stderr_stream.close()

        if return_code != 0:
            stderr_contents = self.__read_stderr()
            raise RuntimeError("collectl did not return a status code of 0, process standard error was %s" % stderr_contents)

    def __read_stderr(self):
        file = open(self.stderr_file, 'r')
        try:
            contents = file.read()
            return contents
        finally:
            file.close()

    def output_file(self):
        return self.collectl_output_file[1]

    def remove_output_file(self):
        os.remove(self.collectl_output_file[1])


class TestCollectlData:
    line1 = "#Test Line"
    line2 = "20110818	00:02:00	22733	31062	20	22686	0	S	12620	0	1756	344	88	672	1940	2	0.00	0.00	0	0:00.00	0	0	0	0	0	0	0	0	0	/opt/bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC"
    line3 = "20110818	00:04:00	22733	31062	20	22686	0	S	12620	0	1756	344	88	672	1940	2	0.00	0.00	0	0:00.00	0	0	0	0	0	0	0	0	0	/opt/bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC"
    line4 = "20110818	00:04:00	22737	31062	20	22686	0	S	12620	0	1756	344	88	672	1940	2	0.00	0.00	0	0:00.00	0	0	0	0	0	0	0	0	0	/opt/bin/cat"
    line5 = "20110818	00:05:00	22737	31062	20	22686	0	S	12620	0	1756	344	88	672	1940	2	0.00	0.00	0	0:00.00	0	0	0	0	0	0	0	0	0	/opt/bin/cat"
    line6 = "20110818	00:06:00	22734	31062	20	22686	0	S	12620	0	1756	344	88	672	1940	2	0.00	0.00	0	0:00.00	0	0	0	0	0	0	0	0	0	/opt/bin/ls"

    @staticmethod
    def make_temp_file():
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.write("%s\n" % TestCollectlData.line1)
        temp_file.write("%s\n" % TestCollectlData.line2)
        temp_file.write("%s\n" % TestCollectlData.line3)
        temp_file.write("%s\n" % TestCollectlData.line4)
        temp_file.write("%s\n" % TestCollectlData.line5)
        temp_file.write("%s\n" % TestCollectlData.line6)
        temp_file.flush()
        return temp_file


class CollectlSummary:
    """
    >>> temp_file = TestCollectlData.make_temp_file()
    >>> parser = CollectlSummary(temp_file.name)
    >>> execution_values = parser.build()
    >>> to_postgres_date(parser.first_date_time)
    '2011-08-18 00:02:00'
    >>> to_postgres_date(parser.last_date_time)
    '2011-08-18 00:06:00'
    >>> executions = parser.get_executions()
    >>> executions['22737 31062 /opt/bin/cat'][5]
    0
    >>> executions['22733 31062 /opt/bin/bash'][5]
    2
    >>> executions['22734 31062 /opt/bin/ls'][5]
    3
    >>> empty_temp_file = tempfile.NamedTemporaryFile()
    >>> empty_temp_file.write("No records")
    >>> empty_temp_file.flush()
    >>> parser = CollectlSummary(empty_temp_file.name)
    >>> execution_values = parser.build()
    >>> len(execution_values)
    0
    """

    def __init__(self, collectl_output):
        self.collectl_output = collectl_output
        self.first_date_time = None
        self.last_date_time = None

    def register_date_time(self, date_time):
        if date_time is None:
            return
        if self.first_date_time is None:
            self.first_date_time = date_time
        self.last_date_time = date_time

    def build(self):
        self.raw_executions = {}
        with open(self.collectl_output, 'r') as file:
            for line in file:
                if self.__valid_line(line):
                    self.register_date_time(CollectlSummary.add_line(line, self.raw_executions))
        return self.get_executions().values()

    def __valid_line(self, line):
        return re.match("\d{8}\s\d\d:\d\d:\d\d", line) is not None

    def get_executions(self):
        executions = self.raw_executions
        for unique_index, execution in executions.items():
            execution.append(self.border_execution(execution))
        return executions

    def border_execution(self, execution):
        """
        Execution occurs during first or last timestamp.
        """
        cutoff_at_start = execution[0] == self.first_date_time
        cutoff_at_end = execution[1] == self.last_date_time
        if cutoff_at_start and cutoff_at_end:
            return DateCutoffType.both
        elif cutoff_at_start:
            return DateCutoffType.start
        elif cutoff_at_end:
            return DateCutoffType.end
        else:
            return DateCutoffType.none

    @staticmethod
    def add_line(line, executions):
        """
        >>> executions = {}
        >>> CollectlSummary.add_line(TestCollectlData.line1, executions)
        >>> executions.values()
        []
        >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line2, executions))
        '2011-08-18 00:02:00'
        >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line3, executions))
        '2011-08-18 00:04:00'
        >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line4, executions))
        '2011-08-18 00:04:00'
        >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line5, executions))
        '2011-08-18 00:05:00'
        >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line6, executions))
        '2011-08-18 00:06:00'
        >>> to_postgres_date(executions["22733 31062 /opt/bin/bash"][0])
        '2011-08-18 00:02:00'
        >>> to_postgres_date(executions["22733 31062 /opt/bin/bash"][1])
        '2011-08-18 00:04:00'
        >>> executions["22733 31062 /opt/bin/bash"][2]
        '22733'
        >>> executions["22733 31062 /opt/bin/bash"][3]
        '31062'
        >>> executions["22733 31062 /opt/bin/bash"][4]
        '/opt/bin/bash'
        >>> to_postgres_date(executions["22734 31062 /opt/bin/ls"][0])
        '2011-08-18 00:06:00'
        >>> to_postgres_date(executions["22734 31062 /opt/bin/ls"][1])
        '2011-08-18 00:06:00'
        """

        # Skip comments
        if(line.find("#") == 0):
            return None

        parsed_line = CollectlSummary.parse_line(line)
        date_time = parsed_line[0]
        pid = parsed_line[1]
        uid = parsed_line[2]
        executable = parsed_line[3]
        if filter_executable(executable) or filter_uid(uid):
            return None
        if re.match('^(\d+|-bash)$', executable) is not None:
            print "Bad line found [%s]" % line
        unique_index = "%s %s %s" % (pid, uid, executable)
        if unique_index in executions:
            execution = executions.get(unique_index)
            # update guess of stop time
            execution[1] = date_time
        else:
            # start, stop, pid, uid, executable
            executions[unique_index] = ([date_time, date_time, pid, uid, executable])
        return date_time

    @staticmethod
    def parse_line(line):
        r"""

        >>> parts = CollectlSummary.parse_line('20110818\t00:02:00\t22733\t31062\t20\t22686\t0\tS\t12620\t0\t1756\t344\t88\t672\t1940\t2\t0.00\t0.00\t0\t0:00.00\t0\t0\t0\t0\t0\t0\t0\t0\t0\t/opt/bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC')
        >>> to_postgres_date(parts[0])
        '2011-08-18 00:02:00'
        >>> parts[1].strip()
        '22733'
        >>> parts[2].strip()
        '31062'
        >>> parts[3].strip()
        '/opt/bin/bash'
        >>> parts = CollectlSummary.parse_line('20110925\t16:01:00\t10951\t11257\t20\t7456\t0\tR\t615788\t100532\t507020\t536368\t5468\t2356\t4180\t1\t1.16\t58.05\t98\t\t\t1165:21\t0\t0\t0\t0\t0\t0\t0\t0\t13K\t./mpcugles              ')
        >>> parts[3].strip()
        './mpcugles'
        >>> parts = CollectlSummary.parse_line('20110925\t16:01:00\t10951\t11257\t20\t7456\t0\tR\t615788\t100532\t507020\t536368\t5468\t2356\t4180\t1\t1.16\t58.05\t98\t\t\t1165:21\t0\t0\t0\t0\t0\t0\t0\t0\t13K\tmpcugles\t./mpcugles              ')
        >>> parts[3].strip()
        './mpcugles'
        >>> parts = CollectlSummary.parse_line('20110925\t16:01:00\t10951\t11257\t20\t7456\t0\tR\t615788\t100532\t507020\t536368\t5468\t2356\t4180\t1\t1.16\t58.05\t98\t\t\t1165:21\t0\t0\t0\t0\t0\t0\t0\t0\t13K\t/opt/mpi/mpcugles\t./mpcugles              ')
        >>> parts[3].strip()
        '/opt/mpi/mpcugles'
        """

        first_line_parts = line.split("\t", 19)
        if len(first_line_parts) != 20:
            raise AssertionError("Failed to parse line [%s]" % line)
        time_str = "%s %s" % (first_line_parts[0], first_line_parts[1])
        rest_line = first_line_parts[19]
        rest_line = rest_line.strip()
        rest_line_parts = rest_line.split("\t", 10)
        timestamp = CollectlSummary.parse_timestamp(time_str)
        pid = first_line_parts[2]
        uid = first_line_parts[3]
        executable = CollectlSummary.parse_program(rest_line_parts[10])
        return (timestamp, pid, uid, executable)

    @staticmethod
    def parse_timestamp(date_str):
        """
        >>> time_tuple = CollectlSummary.parse_timestamp('20110831 15:29:59')
        >>> time.mktime(time_tuple)
        1314822599.0
        """
        return time.strptime(date_str, '%Y%m%d %H:%M:%S')

    @staticmethod
    def parse_program(end_of_collectl_output):
        if end_of_collectl_output.find("\t") != -1:
            # Have an executable and command line string
            (executable, command_line) = end_of_collectl_output.split("\t", 1)
            executable_from_executable_column = executable
            executable_from_command_line = CollectlSummary.parse_program_from_command_line(command_line)
            if CollectlSummary.is_path_to_executable(executable_from_command_line) and not CollectlSummary.is_path_to_executable(executable_from_executable_column):
                return executable_from_command_line
            else:
                return executable_from_executable_column
        else:
            # Only have a command line string, just take the first part of it
            return CollectlSummary.parse_program_from_command_line(end_of_collectl_output)

    @staticmethod
    def is_path_to_executable(executable):
        return executable.find("/") != -1

    @staticmethod
    def parse_program_from_command_line(end_of_collectl_output):
        return end_of_collectl_output.split(None, 1)[0]


class CollectlSummaryFactory:

    def build_for(self, output_file):
        summary = CollectlSummary(output_file)
        return summary.build()


class MockCollectlSummaryFactory:

    def __init__(self, expected_output_file):
        self.expected_output_file = expected_output_file

    def build_for(self, output_file):
        assert self.expected_output_file == output_file
        temp_file = TestCollectlData.make_temp_file()
        parser = CollectlSummary(temp_file.name)
        return parser.build()


class BaseCollectlSqlDumper:

    def dump(self, executions, host, file):
        statements = []
        for execution in executions:
            for statement in self.get_statements_for_execution(execution, host):
                statements.append(statement)
        num_statements = len(statements)
        #if num_statements == 0:
        print "WARNING: Skipping file %s - yielded no statements." % file
        statements.append("INSERT INTO PROCESSED_COLLECTL_LOGS (NAME, NUM_STATEMENTS) VALUES ('%s', %d);" % (escape_quotes(file), num_statements))
        self.handle_statements(statements)

    def get_statements_for_execution(self, execution, host):
        start = execution[0]
        postgres_start = to_postgres_date(start)
        end = execution[1]
        postgres_end = to_postgres_date(end)
        pid = execution[2]
        uid = execution[3]
        executable = execution[4]
        cutoff_type = execution[5]
        statements = []
        if cutoff_type == DateCutoffType.none:
            insert_template = "INSERT INTO RAW_COLLECTL_EXECUTIONS (START_TIME, END_TIME, PID, UID, EXECUTABLE, HOST) VALUES ('%s', '%s', %s, %s, '%s', '%s');"
            insert_statement = (insert_template % (postgres_start, postgres_end, pid, uid, escape_quotes(executable), host))
            #self.handle_statement(insert_statement)
            statements.append(insert_statement)
        else:
            same_execution_condition = BaseCollectlSqlDumper.same_execution_condition(execution, host)
            insert_template = "INSERT INTO RAW_COLLECTL_EXECUTIONS (START_TIME, END_TIME, PID, UID, EXECUTABLE, HOST) SELECT '%s', '%s', %s, %s, '%s', '%s' WHERE 1 NOT IN (SELECT 1 FROM RAW_COLLECTL_EXECUTIONS WHERE %s);"
            insert_statement = (insert_template % (postgres_start, postgres_end, pid, uid, escape_quotes(executable), host, same_execution_condition))
            #self.handle_statement(insert_statement)
            statements.append(insert_statement)
            if cutoff_type == DateCutoffType.end or cutoff_type == DateCutoffType.both:
                update_start_template = "UPDATE RAW_COLLECTL_EXECUTIONS SET START_TIME = '%s' WHERE ID IN (SELECT ID FROM RAW_COLLECTL_EXECUTIONS WHERE %s AND START_TIME > '%s');"
                update_start_statement = update_start_template % (postgres_start, same_execution_condition, postgres_start)
                #self.handle_statement(update_start_statement)
                statements.append(update_start_statement)

            if cutoff_type == DateCutoffType.start or cutoff_type == DateCutoffType.both:
                update_end_template = "UPDATE RAW_COLLECTL_EXECUTIONS SET END_TIME = '%s' WHERE ID IN (SELECT ID FROM RAW_COLLECTL_EXECUTIONS WHERE %s AND END_TIME < '%s');"
                update_end_statement = update_end_template % (postgres_end, same_execution_condition, postgres_end)
                #self.handle_statement(update_end_statement)
                statements.append(update_end_statement)
        return statements

    @staticmethod
    def same_execution_condition(execution, host):
        """
        >>> execution = [CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/opt/bin/cat', True]
        >>> condition = BaseCollectlSqlDumper.same_execution_condition(execution, 'itasca0001')
        >>> condition
        "HOST = 'itasca0001' AND PID = '123' AND UID = '456' AND EXECUTABLE = '/opt/bin/cat' AND START_TIME > '2011-08-04 00:02:00' AND END_TIME < '2011-09-01 00:04:00'"
        """
        start = execution[0]
        end = execution[1]
        pid = execution[2]
        uid = execution[3]
        executable = execution[4]

        same_execution_condition_template = "HOST = '%s' AND PID = '%s' AND UID = '%s' AND EXECUTABLE = '%s' AND START_TIME > '%s' AND END_TIME < '%s'"
        start_datetime = datetime(*start[0:6])
        end_datetime = datetime(*end[0:6])
        one_week = timedelta(weeks=2)
        condition = same_execution_condition_template % (host, pid, uid, executable, to_postgres_date(start_datetime - one_week), to_postgres_date(end_datetime + one_week))
        return condition


class PostgresConnectionFactory:
    def __init__(self, options):
        self.options = options

    def get_database_connection(self):
        import psycopg2
        options = self.options
        return psycopg2.connect(database=options.pg_database, user=options.pg_username, password=options.pg_password, host=options.pg_host, port=options.pg_port)


class PostgresCollectlSqlDumper(BaseCollectlSqlDumper):

    def __init__(self, options):
        self.postgres_connection_factory = PostgresConnectionFactory(options)

    def handle_statements(self, statements):
        connection = self.postgres_connection_factory.get_database_connection()
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
        cursor.close()
        connection.commit()


class StreamCollectlSqlDumper(BaseCollectlSqlDumper):
    """
    >>> import StringIO
    >>> def dump(e): output = StringIO.StringIO(); sql_dumper = StreamCollectlSqlDumper(output); sql_dumper.dump(e, 'itasca0001', '/path/to/file'); return output.getvalue()
    >>> output = dump([(CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/opt/bin/cat', 0)])
    >>> execution = [CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/opt/bin/cat', 1]
    >>> output = dump([execution])
    >>> output.find('''INSERT INTO RAW_COLLECTL_EXECUTIONS (START_TIME, END_TIME, PID, UID, EXECUTABLE, HOST) SELECT '2011-08-18 00:02:00', '2011-08-18 00:04:00', 123, 456, '/opt/bin/cat', 'itasca0001' WHERE ''') >= 0
    True
    >>> output.find('''UPDATE RAW_COLLECTL_EXECUTIONS SET START_TIME = '2011-08-18 00:02:00' WHERE ID IN (SELECT ID FROM ''') >= 0
    True
    >>> output.find('''UPDATE RAW_COLLECTL_EXECUTIONS SET END_TIME = '2011-08-18 00:04:00' WHERE ID IN (SELECT ID FROM ''') >= 0
    True
    >>> execution[5] = 2 # just start
    >>> output = dump([execution])
    >>> output.find('''SET START_TIME''') >= 0
    False
    >>> output.find('''SET END_TIME''') >= 0
    True
    >>> execution[5] = 3 # update END_TIME DATE if needed
    >>> output = dump([execution])
    >>> output.find('''SET START_TIME''') >= 0
    True
    >>> output.find('''SET END_TIME''') >= 0
    False
    """

    def __init__(self, stream=sys.stdout):
        self.stream = stream
        self.lock = threading.Lock()

    def handle_statements(self, statements):
        with self.lock:
            for statement in statements:
                print >> self.stream, statement


class CollectlSqlDumperFactroy:

    def __init__(self, options):
        if options.output_type == "stdout":
            self.sql_dumper = StreamCollectlSqlDumper()
        else:
            self.sql_dumper = PostgresCollectlSqlDumper(options)

    def get_dumper(self):
        return self.sql_dumper


class CollectlExecutionMerger:
    """
    In an effort to reduce records produced, merge executions that only occur at one timestamp and have the same executable and uid.

    >>> def get_count(executions): merger = CollectlExecutionMerger(); merger.merge(executions); return len(merger.get_merged_executions())
    >>> def test_time(str_time = '20110818 00:02:00'): return CollectlSummary.parse_timestamp(str_time)
    >>> get_count([[test_time(), CollectlSummary.parse_timestamp('20110818 00:03:00'), 3, 1, 'g09' ]])
    1
    >>> get_count([[test_time(), test_time(), 3, 1, 'g09'],[test_time(), test_time(), 4, 1, 'g09x']] )
    2
    >>> get_count([[test_time(), test_time(), 3, 1, 'g09'],[test_time(), test_time(), 4, 2, 'g09']] )
    2
    >>> get_count([[test_time(), test_time(), 3, 1, 'g09'],[test_time(), test_time(), 4, 1, 'g09']] )
    1
    >>> get_count([[test_time(), test_time(), 3, 1, 'l501.exel'],[test_time(), test_time(), 4, 1, 'l501.exel']] )
    1
    >>> get_count([[test_time(), test_time(), 3, 1, '/bin/ls'],[test_time(), test_time(), 4, 1, '/bin/ls']] )
    2
    >>> get_count([[test_time('20110818 00:04:00'), test_time('20110818 00:04:00'), 3, 1, 'g09'],[test_time(), test_time(), 4, 1, 'g09']] )
    1
    """

    def __init__(self):
        self.merged_executions = []

    def merge(self, executions):
        execution_clusters = []
        for execution in executions:
            if not self.mergeable_execution(execution):
                self.__append_execution(execution)
            else:
                execution_clusters.append(execution)
        for execution in self.__merge_execution_clusters(execution_clusters):
            self.__append_execution(execution)

    def mergeable_execution(self, execution):
        return (execution[0] == execution[1]) and merge_executable(execution[4])

    def __append_execution(self, execution):
        self.merged_executions.append(execution)

    def __merge_execution_clusters(self, execution_clusters):
        while True:
            iteration_execution_clusters = []
            for execution in execution_clusters:
                match = None
                for iteration_execution in iteration_execution_clusters:
                    if self.__can_merge(execution, iteration_execution):
                        match = iteration_execution
                        break
                if match is not None:
                    iteration_execution_clusters.remove(match)
                    new_execution = self.__merge(execution, match)
                    iteration_execution_clusters.append(new_execution)
                else:
                    iteration_execution_clusters.append(execution)
            if len(iteration_execution_clusters) == len(execution_clusters):
                break
            else:
                execution_clusters = iteration_execution_clusters
        return execution_clusters

    def __can_merge(self, execution1, execution2):
        uid1 = execution1[3]
        uid2 = execution2[3]
        executable1 = execution1[4]
        executable2 = execution2[4]
        if not (uid1 == uid2 and executable1 == executable2):
            return False
        start1 = datetime(*execution1[0][0:6])
        start2 = datetime(*execution2[0][0:6])
        end1 = datetime(*execution1[1][0:6])
        end2 = datetime(*execution2[1][0:6])

        five_minutes = timedelta(minutes=5)

        merge_before = abs(start1 - end2) < five_minutes
        merge_after = abs(start2 - end1) < five_minutes
        merge_contained = (start1 >= start2 and end1 <= end2) or (start2 >= start1 and end2 <= end1)
        return merge_before or merge_after or merge_contained

    def __merge(self, execution1, execution2):
        min_start = min(execution1[0], execution2[0])
        max_end = max(execution1[1], execution2[1])
        execution1[0] = min_start
        execution1[1] = max_end
        return execution1

    def get_merged_executions(self):
        return self.merged_executions


class CollectlFileScanner:
    """
    """

    def __stderr_file(self, log_file_path_prefix):
        return os.path.join(os.path.join(self.options.log_directory, self.host), log_file_path_prefix + "-stderr")

    def __init__(self, options, host, node_name, rawp_file, relative_file_path, log_recorder):
        self.options = options
        self.host = host
        self.node_name = node_name
        self.rawp_file = rawp_file
        stderr_path = None
        if not options.use_db():
            stderr_path = self.__stderr_file(relative_file_path)
        self.rawp_file = rawp_file
        self.stderr_path = stderr_path
        self.collectl_path = options.collectl_path
        self.collectl_summary_factory = CollectlSummaryFactory()
        self.collectl_sql_dumper_factory = CollectlSqlDumperFactroy(options)
        self.relative_file_path = relative_file_path
        self.log_recorder = log_recorder

    def log_start(self):
        self.log_recorder.log_start(self.relative_file_path)

    def log_end(self):
        self.log_recorder.log_end(self.relative_file_path)

    def execute(self, collectl_executor_factory):
        self.log_start()
        executor = collectl_executor_factory.get_collectl_executor(self.rawp_file, self.stderr_path)
        executor.execute_collectl()
        collectl_output_file = executor.output_file()
        executions = self.collectl_summary_factory.build_for(collectl_output_file)
        executor.remove_output_file()
        execution_merger = CollectlExecutionMerger()
        execution_merger.merge(executions)
        executions = execution_merger.get_merged_executions()
        self.collectl_sql_dumper_factory.get_dumper().dump(executions, self.node_name, self.rawp_file)
        self.log_end()


class FileLogRecorder:

    def __init__(self, options, host):
        self.options = options
        self.host = host

    def __log_file_base(self, dir_file):
        return os.path.join(os.path.join(self.options.log_directory, self.host), dir_file)

    def previously_recorded(self, filename):
        return os.path.exists(self.__parsing_completed_file(self.__log_file_base(filename)))

    def log_start(self, relative_file_path):
        self.__touch(self.__parsing_started_file(self.__log_file_base(relative_file_path)))

    def log_end(self, relative_file_path):
        self.__touch(self.__parsing_completed_file(self.__log_file_base(relative_file_path)))

    def __parsing_started_file(self, log_file_path_prefix):
        return log_file_path_prefix + "-parsing-started"

    def __parsing_completed_file(self, log_file_path_prefix):
        return log_file_path_prefix + "-parsing-completed"

    def __touch(self, file_name):
        parent_directory = os.path.dirname(file_name)
        if not os.path.exists(parent_directory):
            os.makedirs(parent_directory)
        with file(file_name, 'a'):
            os.utime(file_name, None)


class DbLogRecorder:
    def __init__(self, options, host):
        self.options = options
        self.host = host
        self.connection = PostgresConnectionFactory(options).get_database_connection()

    def previously_recorded(self, filename):
        full_path = os.path.join(os.path.join(self.options.directory, self.host), filename)
        cursor = self.connection.cursor()
        cursor.execute("select 1 from PROCESSED_COLLECTL_LOGS where NAME = %s", (full_path,))
        found_row = cursor.fetchone() is not None
        cursor.close()
        return found_row

    def log_start(self, filename):
        pass

    def log_end(self, filename):
        pass


class LogRecorder:

    def __init__(self, options, host):
        if options.use_db():
            self.delegate = DbLogRecorder(options, host)
        else:
            self.delegate = FileLogRecorder(options, host)

    def previously_recorded(self, filename):
        return self.delegate.previously_recorded(filename)

    def log_start(self, filename):
        self.delegate.log_start(filename)

    def log_end(self, filename):
        self.delegate.log_end(filename)


def build_consumers(queue, options):
    consumers_config = read_yaml("consumers.yaml")
    consumers = []
    for consumer_config in consumers_config["consumers"]:
        host = consumer_config.get("host", "localhost")
        user = consumer_config.get("user", getpass.getuser())
        threads = consumer_config.get("threads", 1)
        consumer = CollectlConsumer(queue, options, host, user, threads)
        consumers.append(consumer)
    return consumers


def read_yaml(yaml_file):
    with open(yaml_file) as in_handle:
        return yaml.load(in_handle)


class CollectlConsumer:

    def __init__(self, queue, options, host, user, thread_count):
        self.queue = queue
        self.options = options
        self.collectl_executor_factory = LocalCollectlExecutorFactory(host, user, options.collectl_path)
        for i in range(thread_count):
            t = threading.Thread(target=self.execute_file_parser)
            t.daemon = True
            t.start()

    def execute_file_parser(self):
        while True:
            file_parser = self.queue.get()
            try:
                file_parser.execute(self.collectl_executor_factory)
            except Exception, err:
                sys.stderr.write('ERROR: %s\n' % str(err))
            finally:
                self.queue.task_done()


class CollectlDirectoryScanner:
    """
    >>> import tempfile, shutil, os
    >>> temp_date_dir_base = tempfile.mkdtemp()
    >>> temp_date_dir = os.path.join(temp_date_dir_base, "itasca")
    >>> os.mkdir(temp_date_dir)
    >>> temp_log_dir = tempfile.mkdtemp()
    >>> open(temp_date_dir + "/node0506-20110819-000100.rawp.gz", 'w').close()
    >>> open(temp_date_dir + "/node0506-20110819-000100.raw.gz", 'w').close()
    >>> open(temp_date_dir + "/node0507-20110819-000100.rawp.gz", 'w').close()
    >>> open(temp_date_dir + "/node0507-20110819-000100.raw.gz", 'w').close()
    >>> itasca_log_dir = os.path.join(temp_log_dir, "itasca")
    >>> os.makedirs(itasca_log_dir)
    >>> options = TestCollectlParseOptions(date='20110819', directory=temp_date_dir_base, log_directory=temp_log_dir, batch_size=None, output_type='stdout', collectl_path=None)
    >>> open(os.path.join(itasca_log_dir, "node0507-20110819-000100.rawp.gz-parsing-completed"), 'w').close() # Mark file as previously processed
    >>> dated_dir_scanner = CollectlDirectoryScanner(options, "itasca")
    >>> nodes = dated_dir_scanner.get_node_scanners()
    >>> file_scanner = nodes.next()
    >>> file_scanner.node_name
    'itascanode0506'
    >>> nodes.next()
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    StopIteration
    >>> open(temp_date_dir + "/node0506-20110820-000100.rawp.gz", 'w').close()
    >>> open(temp_date_dir + "/node0506-20110820-000100.raw.gz", 'w').close()
    >>> options =  TestCollectlParseOptions(date=None, directory=temp_date_dir_base, host_prefix='itasca', log_directory=temp_log_dir, batch_size=None, output_type='stdout',collectl_path=None)
    >>> undated_dir_scanner = CollectlDirectoryScanner(options, "itasca")
    >>> nodes = undated_dir_scanner.get_node_scanners()
    >>> files = [os.path.basename(nodes.next().rawp_file), os.path.basename(nodes.next().rawp_file)]
    >>> files.sort()
    >>> files
    ['node0506-20110819-000100.rawp.gz', 'node0506-20110820-000100.rawp.gz']
    >>> shutil.rmtree(temp_date_dir_base)
    >>> shutil.rmtree(temp_log_dir)
    """

    def get_node_scanners(self):
        potential_files = []
        if self._is_directory(self.directory):
            potential_files.extend(self._list_dir(self.directory, append_dir_file=False))
        while len(potential_files) > 0 and (self.batch_size == None or self.batch_count < self.batch_size):
            dir_file = potential_files.pop()
            if self._is_directory(self.__get_path(dir_file)):
                potential_files.extend(self._list_dir(dir_file))
            elif self.__do_parse_file(dir_file):
                self.batch_count = self.batch_count + 1
                yield self.__build_file_parser(dir_file)

    def _is_directory(self, path):
        # Small optimization don't hit file system (local or via fabric) if this
        # doesn't look like a directory
        if path.endswith('.gz'):
            is_directory = False
        else:
            is_directory = file_is_dir(path)
        return is_directory

    def _list_dir(self, dir_file, append_dir_file=True):
        directory_path = os.path.join(self.directory, dir_file)

        with cd(directory_path):
            ls_str = c_run("/bin/bash -c 'ls | awk \"{ print $1 \"}'").strip()
            if not ls_str:
                return []
            ls_files = ls_str.split("\n")
            if append_dir_file:
                return [os.path.join(dir_file, line.strip()) for line in ls_files]
            else:
                return [line.strip() for line in ls_files]

    def execute(self):
        self.queue = Queue.Queue(32)
        self.all_items_added = False

        build_consumers(self.queue, self.options)

        for file_parser in self.get_node_scanners():
            self.queue.put(file_parser, True)

        self.queue.join()

    def __do_parse_file(self, dir_file):
        return self.__filename_matches(dir_file) and \
            not self.__previously_parsed(dir_file) and \
            self.__on_date_range(dir_file)

    def __get_path(self, dir_file):
        return os.path.join(self.directory, dir_file)

    def __filename_matches(self, dir_file):
        date_match = "\w+"
        if has_text(self.date):
            date_match = self.date
        do_parse = re.match("^(.*/)?\w+-%s-\w+.rawp.gz$" % date_match, dir_file)
        return do_parse is not None

    def __get_time_from_filename(self, file_name):
        match = re.match("^(.*/)?\w+-(\w+)-\w+.rawp.gz$", file_name)
        return int(datetime.strptime(match.group(2), "%Y%m%d").strftime("%s"))

    def __on_date_range(self, dir_file):
        from_date = self.options.from_date
        to_date = self.options.to_date
        on_range = True
        if from_date or to_date:
            modified_seconds = self.__get_time_from_filename(dir_file)
            modified_time = time.localtime(modified_seconds)
            if from_date and (from_date > modified_time):
                on_range = False
            elif to_date and (to_date < modified_time):
                on_range = False
        #print "On range (%s, %s): %s -> %s " % (str(from_date), str(to_date), dir_file, str(on_range))
        return on_range

    def __previously_parsed(self, dir_file):
        return self.log_recorder.previously_recorded(dir_file)

    def __node_name(self, rawp_file):
        return "%s%s" % (self.host_prefix, os.path.basename(rawp_file).split("-")[0])

    def __build_file_parser(self, rawp_file):
        return CollectlFileScanner(self.options, self.host_prefix, self.__node_name(rawp_file), os.path.join(self.directory, rawp_file), rawp_file, self.log_recorder)

    def __init__(self, options, host):
        self.options = options
        self.date = options.date
        self.directory = os.path.join(options.directory, host)
        self.host_prefix = host
        self.batch_size = options.batch_size
        self.log_recorder = LogRecorder(options, host)
        self.batch_count = 0


def has_text(input):
    """
    >>> not has_text('')
    True
    >>> not has_text(None)
    True
    >>> not has_text('Hello World!')
    False
    """
    return not input is None and not input == ''


class BaseCollectlParseOptions:

    def use_db(self):
        return self.output_type == "postgres"


class TestCollectlParseOptions(BaseCollectlParseOptions):

    def __init__(self, **dynamic_options):
        for key, value in dynamic_options.iteritems():
            self.__dict__[key] = value


class CollectlParseOptions(BaseCollectlParseOptions):

    def __get_database_option(self, parser, option_name, default=None):
        option = default
        environment_variable = option_name.upper()
        if parser.__dict__.has_key(option_name):
            option = parser.__dict__[option_name]
        elif os.environ.has_key(environment_variable):
            option = os.environ[environment_variable]
        if option is None:
            parser.error("output_type postgres chosen, but failed to specify database parameter %s and could not find fallback value specified as environment variable %s" % (option_name, environment_variable))
        return option

    def __init__(self):
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("--date", dest="date", help="Date in format YYYYMMDD")
        parser.add_option("--from", dest="from_date", help="Date in format YYYYMMDD")
        parser.add_option("--to", dest="to_date", help="Date in format YYYYMMDD")
        parser.add_option("--hosts", dest="hosts", help="A comma-separated list of subdirectories to search beneath specified directory (e.g. itasca,calhoun,koronis,elmo)")
        parser.add_option("--directory", dest="directory", help="Directory to scan for collectl files")
        parser.add_option("--batch_size", dest="batch_size", help="Maximum number of files to parse.", type="int", default=None)
        parser.add_option("--output_type", dest="output_type", help="Output type (e.g. postgres, stdout).", default="postgres", choices=["stdout", "postgres"])
        parser.add_option("--log_directory", dest="log_directory", help="Directory to record information about which files have been processed, used only if output type is stdout.", default="collectl_parse_log")
        parser.add_option("--collectl_path", dest="collectl_path", help="Path to collectl script/executable, if not specified this should be on the caller's PATH", default=None)

        parser.add_option("--remote_host")
        parser.add_option("--remote_user")
        parser.add_option("--ssh_key")

        parser.add_option("--pg_database", dest="pg_database", default=None)
        parser.add_option("--pg_username", dest="pg_username", default=None)
        parser.add_option("--pg_password", dest="pg_password", default=None)
        parser.add_option("--pg_port", dest="pg_port", default=None)
        parser.add_option("--pg_host", dest="pg_host", default=None)

        parser.add_option("--check_database_connection", dest="check_database_connection", help="Check database connection before beginning parse.", action="store_true", default=False)
        parser.add_option("--check_args_only", dest="check_args_only", help="Do not run anything, just check that passed arguments are valid.", action="store_true", default=False)

        (options, args) = parser.parse_args()
        self.date = options.date
        self.directory = options.directory
        self.log_directory = options.log_directory
        self.batch_size = options.batch_size
        self.output_type = options.output_type
        self.scan = not options.check_args_only
        self.check_database_connection = options.check_database_connection
        self.collectl_path = options.collectl_path

        self.remote_host = options.remote_host

        self.remote_user = options.remote_user
        if not self.remote_user:
            self.remote_user = os.environ['USER']

        self.ssh_key = options.ssh_key
        if not self.ssh_key:
            self.ssh_key = os.path.join(os.environ['HOME'], '.ssh', 'id_dsa')

        if not has_text(options.hosts):
            parser.error("Argument hosts not specified.")
        self.hosts = options.hosts.split(",")

        if has_text(options.from_date):
            self.from_date = self.__parse_time(options.from_date)
        else:
            self.from_date = None

        if has_text(options.to_date):
            self.to_date = self.__parse_time(options.to_date)
        else:
            self.to_date = None

        if not has_text(self.directory):
            parser.error("Directory to scan not specified.")
        #if not os.path.isdir(self.directory):
        #    parser.error("Invalid directory specified - %s does not appear to be directory." % self.directory)
        if self.output_type == "postgres":
            self.pg_database = self.__get_database_option(parser, "pg_database")
            self.pg_username = self.__get_database_option(parser, "pg_username")
            self.pg_password = self.__get_database_option(parser, "pg_password")
            self.pg_host = self.__get_database_option(parser, "pg_host", "localhost")
            self.pg_port = self.__get_database_option(parser, "pg_port")

    def __parse_time(self, date_str):
        return time.strptime(date_str, '%Y%m%d')


def main():
    options = CollectlParseOptions()
    if options.check_database_connection:
        PostgresCollectlSqlDumper(options)
    if options.remote_host:
        env.user = options.remote_user
        env.host_string = options.remote_host
        env.key_filename = options.ssh_key
        env.disable_known_hosts = True
        mode_user()
    else:
        print "Mode local"
        mode_local()
    if options.scan:
        for host in options.hosts:
            print "Handling host %s" % host
            CollectlDirectoryScanner(options, host).execute()

if __name__ == "__main__":
    main()
