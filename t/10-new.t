#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 6;

use DBI;
use Queue::DBI;


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
	'Queue::DBI',
	'new',
);

# Instantiate the queue object.
subtest(
	'Verify mandatory argments.',
	sub
	{
		plan( tests => 2 );
		
		dies_ok(
			sub
			{
				Queue::DBI->new(
					'database_handle' => $dbh,
					'cleanup_timeout' => 3600,
					'verbose'         => 0,
				);
			},
			'The argument "queue_name" is required.',
		);
		
		dies_ok(
			sub
			{
				Queue::DBI->new(
					'queue_name'      => 'test1',
					'cleanup_timeout' => 3600,
					'verbose'         => 0,
				);
			},
			'The argument "database_handle" is required.',
		);
	}
);

my $queue;
lives_ok(
	sub
	{
		$queue = Queue::DBI->new(
			'queue_name'      => 'test1',
			'database_handle' => $dbh,
			'cleanup_timeout' => 3600,
			'verbose'         => 0,
		);
	},
	'Instantiate a new Queue::DBI object.',
);
isa_ok(
	$queue,
	'Queue::DBI',
	'Object returned by new()',
);

subtest(
	'Verify optional arguments.',
	sub
	{
		plan( tests => 3 );
		
		dies_ok(
			sub
			{
				Queue::DBI->new(
					'queue_name'      => 'test1',
					'database_handle' => $dbh,
					'cleanup_timeout' => 'test',
					'verbose'         => 0,
				);
			},
			'The argument "cleanup_timeout" must be an integer.',
		);
		
		lives_ok(
			sub
			{
				Queue::DBI->new(
					'queue_name'      => 'test1',
					'database_handle' => $dbh,
					'cleanup_timeout' => 3600,
				);
			},
			'The argument "verbose" is optional.',
		);
		
		dies_ok(
			sub
			{
				Queue::DBI->new(
					'queue_name'      => 'test1',
					'database_handle' => $dbh,
					'cleanup_timeout' => 3600,
					'lifetime'        => 'test',
				);
			},
			'The argument "test" must be an integer.',
		);
	}
);
