#!perl

use strict;
use warnings;

use Test::More tests => 14;

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
	'Instantiate the queue.',
) || diag( "Error: $@ " );

ok(
	defined( $queue ) && $queue->isa( 'Queue::DBI' ),
	'The queue is a Queue::DBI object.',
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
	'Retrieve the next element in the queue.',
) || diag( "Error: $@ " );

ok(
	defined( $queue_element ) && $queue_element->isa( 'Queue::DBI::Element' ),
	'The element is a Queue::DBI::Element object.',
) || diag( '$queue: ' . ( defined( $queue_element ) ? 'ref() >' . ref( $queue_element ) .'<' : 'undef' ) );

# Retrieve the queue element by ID.
my $queue_element_by_id;
eval
{
	$queue_element_by_id = $queue->get_element_by_id( $queue_element->id() );
};

ok(
	!$@,
	'Retrieve a queue element by ID.',
) || diag( "Error: $@" );

ok(
	defined( $queue_element_by_id ) && $queue_element_by_id->isa( 'Queue::DBI::Element' ),
	'Queue::DBI::Element object returned',
) || diag( '$queue: ' . ( defined( $queue_element_by_id ) ? 'ref() >' . ref( $queue_element_by_id ) .'<' : 'undef' ) );

is(
	$queue_element_by_id->id(),
	$queue_element->id(),
	'The ID of the element retrieved is correct.',
);

# Lock.
eval
{
	$queue_element->lock()
	||
	die 'Cannot lock element';
};
ok(
	!$@,
	'Lock element.',
) || diag( "Error: $@ " );


# Retrieve the queue element by ID after locking.
my $queue_element_by_id_after_lock;
eval
{
	$queue_element_by_id_after_lock = $queue->get_element_by_id( $queue_element->id() );
};

ok(
	!$@,
	'Retrieve the queue element by ID after locking it.',
) || diag( "Error: $@" );

ok(
	defined( $queue_element_by_id_after_lock ) && $queue_element_by_id_after_lock->isa( 'Queue::DBI::Element' ),
	'The element is a Queue::DBI::Element object.',
) || diag( '$queue_element_by_id_after_lock: ' . ( defined( $queue_element_by_id_after_lock ) ? 'ref() >' . ref( $queue_element_by_id_after_lock ) .'<' : 'undef' ) );

is(
	$queue_element_by_id_after_lock->id(),
	$queue_element->id(),
	'The ID of the element retrieved is correct.',
);

# Remove.
eval
{
	$queue_element->success()
	||
	die 'Cannot mark as successfully processed';
};
ok(
	!$@,
	'Mark the element as successfully processed.',
) || diag( "Error: $@" );
