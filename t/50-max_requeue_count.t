#!perl

use strict;
use warnings;

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
eval
{
	$queue = Queue::DBI->new(
		'queue_name'        => 'test1',
		'database_handle'   => $dbh,
		'cleanup_timeout'   => 3600,
		'verbose'           => 0,
		'max_requeue_count' => 5,
	);
};
ok(
	!$@,
	'Instantiate a new Queue::DBI object.',
) || diag( "Error: $@ " );
ok(
	defined( $queue ) && $queue->isa( 'Queue::DBI' ),
	'The queue is a Queue::DBI object.',
) || diag( '$queue: ' . ( defined( $queue ) ? 'ref() >' . ref( $queue ) .'<' : 'undef' ) );

# Verify that max_requeue_count() returns the correct result.
is(
	$queue->max_requeue_count(),
	5,
	'Retrieve the max_requeue_count.',
);
