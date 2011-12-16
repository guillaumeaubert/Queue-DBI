#!perl

use strict;
use warnings;

use Test::More tests => 8;

use DBI;
use Data::Dumper;
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
		'queue_name'      => 'test1',
		'database_handle' => $dbh,
		'cleanup_timeout' => 3600,
		'verbose'         => 0,
	);
};
ok(
	!$@,
	'Instantiate a new Queue::DBI object',
) || diag( "Error: $@ " );
ok(
	defined( $queue ) && $queue->isa( 'Queue::DBI' ),
	'Queue::DBI object returned',
) || diag( '$queue: ' . ( defined( $queue ) ? 'ref() >' . ref( $queue ) .'<' : 'undef' ) );

# Insert data.
my $data =
{
	block1 => 141592653,
	block2 => 589793238,
	block3 => 462643383,
};
eval
{
	$queue->enqueue( $data );
};
ok(
	!$@,
	'Queue data',
) || diag( "Could not enqueue the following data:\n" . Dumper( $data ) );

# Retrieve data.
my $queue_element;
eval
{
	$queue_element = $queue->next();
};
ok(
	!$@,
	'Call to retrieve the next item in the queue',
) || diag( "Error: $@ " );
ok(
	defined( $queue_element ) && $queue_element->isa( 'Queue::DBI::Element' ),
	'Queue::Safe::Element object returned',
) || diag( '$queue: ' . ( defined( $queue_element ) ? 'ref() >' . ref( $queue_element ) .'<' : 'undef' ) );

# Lock.
eval
{
	$queue_element->lock()
	||
	die 'Cannot lock element';
};
ok(
	!$@,
	'Lock element',
) || diag( "Error: $@ " );

# Remove.
eval
{
	$queue_element->success()
	||
	die 'Cannot mark as successfully processed';
};
ok(
	!$@,
	'Mark as successfully processed.',
) || diag( "Error: $@ " );
