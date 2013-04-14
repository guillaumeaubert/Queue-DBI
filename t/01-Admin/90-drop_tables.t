#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::FailWarnings;
use Test::More tests => 4;

use lib 't/';
use LocalTest;

use Queue::DBI::Admin;


my $dbh = LocalTest::ok_database_handle();

can_ok(
	'Queue::DBI::Admin',
	'drop_tables',
);

subtest(
	'Check default tables.',
	sub
	{
		plan( tests => 4 );
		
		my $queue_admin;
		lives_ok(
			sub
			{
				$queue_admin = Queue::DBI::Admin->new(
					'database_handle' => $dbh,
				);
			},
			'Instantiate a new Queue::DBI::Admin object.',
		);
		
		lives_ok(
			sub
			{
				$queue_admin->drop_tables();
			},
			'Drop the default tables.',
		);
		
		my $tables_exist;
		lives_ok(
			sub
			{
				$tables_exist = $queue_admin->has_tables();
			},
			'Call has_tables().',
		);
		
		ok(
			!$tables_exist,
			'The default tables do not exist anymore.',
		);
	}
);

subtest(
	'Check custom tables.',
	sub
	{
		plan( tests => 4 );
		
		my $queue_admin;
		lives_ok(
			sub
			{
				$queue_admin = Queue::DBI::Admin->new(
					'database_handle'           => $dbh,
					'queues_table_name'         => 'test_queues',
					'queue_elements_table_name' => 'test_queue_elements',
				);
			},
			'Instantiate a new Queue::DBI::Admin object.',
		);
		
		lives_ok(
			sub
			{
				$queue_admin->drop_tables();
			},
			'Drop the custom tables.',
		);
		
		my $tables_exist;
		lives_ok(
			sub
			{
				$tables_exist = $queue_admin->has_tables();
			},
			'Call has_tables().',
		);
		
		ok(
			!$tables_exist,
			'The custom tables do not exist anymore.',
		);
	}
);


