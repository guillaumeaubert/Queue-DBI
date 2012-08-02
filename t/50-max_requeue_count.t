#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 4;

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

# Instantiate the queue object.
my $queue;
lives_ok(
	sub
	{
		$queue = Queue::DBI->new(
			'queue_name'        => 'test1',
			'database_handle'   => $dbh,
			'cleanup_timeout'   => 3600,
			'verbose'           => 0,
			'max_requeue_count' => 5,
		);
	},
	'Instantiate a new Queue::DBI object.',
);
isa_ok(
	$queue,
	'Queue::DBI',
	'Object returned by Queue::DBI->new()',
);

# Verify that max_requeue_count() returns the correct result.
is(
	$queue->max_requeue_count(),
	5,
	'Retrieve the max_requeue_count.',
);
