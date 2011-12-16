#!perl

use strict;
use warnings;

use Test::More tests => 45;
use DBI;
use Data::Dumper;
use Queue::DBI;

# Note: the queue object is designed to never backtrack, so we need to re-create
# the queue object everytime to be able to pick the element we just requeued.

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

# First part, insert the element.
{
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
		'Create the queue object.',
	) || diag( "Error: $@ " );
	ok(
		defined( $queue ) && $queue->isa( 'Queue::DBI' ),
		'The queue is a Queue::DBI object.',
	) || diag( '$queue: ' . ( defined( $queue ) ? 'ref() >' . ref( $queue ) .'<' : 'undef' ) );
	
	# Clean up queue if needed.
	my $removed_elements = 0;
	eval
	{
		while ( my $queue_element = $queue->next() )
		{
			$queue_element->lock() || die 'Could not lock the queue element';
			$queue_element->success() || die 'Could not mark as processed the queue element';
			$removed_elements++;
		}
	};
	ok(
		!$@,
		'Queue is empty',
	) || diag( "Error: $@ " );
	note( "Removed >$removed_elements< elements." )
		if $removed_elements != 0;
	
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
}

# Second part: retrieve, lock and requeue the element. The element should not be
# retrievable the seventh time, as it will have been requeued six times.
#
# Note: we needto re-instantiate the queue each time as the dequeueing algorithm
# prevents loops and we wouldn't be able to retrieve the element again.
foreach my $try ( 1..6 )
{
	note( "<Round $try>" );
	
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
		'Create the queue object.',
	) || diag( "Error: $@ " );
	ok(
		defined( $queue ) && $queue->isa( 'Queue::DBI' ),
		'The queue is a Queue::DBI object.',
	) || diag( '$queue: ' . ( defined( $queue ) ? 'ref() >' . ref( $queue ) .'<' : 'undef' ) );
	
	# Retrieve element.
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
	
	# Requeue.
	eval
	{
		$queue_element->requeue()
		||
		die 'Cannot requeue element';
	};
	ok(
		!$@,
		'Requeue element.',
	) || diag( "Error: $@ " );
	
	note( "</Round $try>" );
}

# Now, the seventh time we try to retrieve the element, it should not be returned.
{
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
		'Create the queue object.',
	) || diag( "Error: $@ " );
	ok(
		defined( $queue ) && $queue->isa( 'Queue::DBI' ),
		'The queue is a Queue::DBI object.',
	) || diag( '$queue: ' . ( defined( $queue ) ? 'ref() >' . ref( $queue ) .'<' : 'undef' ) );
	
	# Retrieve element.
	my $queue_element;
	eval
	{
		$queue_element = $queue->next();
	};
	ok(
		!$@,
		'Retrieve the next element in the queue',
	) || diag( "Error: $@ " );
	ok(
		!defined( $queue_element ),
		'No element returned.',
	) || diag( "Queue element returned:\n" . Dumper( $queue_element ) );
}
