#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 4;

use DBI;
use Queue::DBI::Admin;


ok(
	my $dbh = DBI->connect(
		'dbi:SQLite:dbname=t/01-Admin/test_database',
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
	'has_tables',
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
		
		ok(
			!$queue_admin->has_tables(),
			'The default tables do not exist yet.',
		);
		
		my ( $tables_exist, $missing_tables ) = $queue_admin->has_tables();
		
		ok(
			!$tables_exist,
			'The call in list context indicates that the tables do not exist.',
		);
		
		is_deeply(
			$missing_tables,
			[
				$Queue::DBI::DEFAULT_QUEUES_TABLE_NAME,
				$Queue::DBI::DEFAULT_QUEUE_ELEMENTS_TABLE_NAME,
			],
			'All the tables are missing.',
		) || diag( explain( $missing_tables ) );
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
		
		ok(
			!$queue_admin->has_tables(),
			'The custom tables do not exist yet.',
		);
		
		my ( $tables_exist, $missing_tables ) = $queue_admin->has_tables();
		
		ok(
			!$tables_exist,
			'The call in list context indicates that the tables do not exist.',
		);
		
		is_deeply(
			$missing_tables,
			[
				'test_queues',
				'test_queue_elements',
			],
			'All the tables are missing.',
		) || diag( explain( $missing_tables ) );
	}
);

