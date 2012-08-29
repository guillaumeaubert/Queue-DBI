#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 4;

use DBI;
use Queue::DBI::Admin;


ok(
	my $dbh = DBI->connect(
		'dbi:SQLite:dbname=t/test_database',
		'',
		'',
		{
			RaiseError => 1,
		}
	),
	'Create connection to a SQLite database.',
);

can_ok(
	'Queue::DBI::Admin',
	'get_queues_table_name',
);

subtest(
	'Test using the default queues table name.',
	sub
	{
		plan( tests => 2 );
		
		my $queue_admin;
		lives_ok(
			sub
			{
				$queue_admin = Queue::DBI::Admin->new(
					'database_handle'   => $dbh,
				);
			},
			'Instantiate a new Queue::DBI::Admin object with "queues_table_name" not set.',
		);
		
		is(
			$queue_admin->get_queues_table_name(),
			$Queue::DBI::DEFAULT_QUEUES_TABLE_NAME,
			'The method get_database_handle() returns the default queue table name.',
		);
	}
);

subtest(
	'Test setting a custom queues table name.',
	sub
	{
		plan( tests => 2 );
		
		my $queue_admin;
		lives_ok(
			sub
			{
				$queue_admin = Queue::DBI::Admin->new(
					'database_handle'   => $dbh,
					'queues_table_name' => 'test_queues',
				);
			},
			'Instantiate a new Queue::DBI::Admin object with "queues_table_name" set.',
		);
		
		is(
			$queue_admin->get_queues_table_name(),
			'test_queues',
			'The method get_queues_table_name() returns the queues table name passed to new().',
		);
	}
);
