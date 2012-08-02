#!perl -T

use strict;
use warnings;

use Test::Exception;
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
		'Create the queue object.',
	);
	isa_ok(
		$queue,
		'Queue::DBI',
		'Object returned by Queue::DBI->new()',
	);
	
	# Clean up queue if needed.
	my $removed_elements = 0;
	lives_ok(
		sub
		{
			while ( my $queue_element = $queue->next() )
			{
				$queue_element->lock() || die 'Could not lock the queue element';
				$queue_element->success() || die 'Could not mark as processed the queue element';
				$removed_elements++;
			}
		},
		'Queue is empty.',
	);
	note( "Removed >$removed_elements< elements." )
		if $removed_elements != 0;
	
	# Insert data.
	my $data =
	{
		block1 => 141592653,
		block2 => 589793238,
		block3 => 462643383,
	};
	lives_ok(
		sub
		{
			$queue->enqueue( $data );
		},
		'Queue data.',
	);
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
		'Create the queue object.',
	);
	isa_ok(
		$queue,
		'Queue::DBI',
		'Object returned by Queue::DBI->new()',
	);
	
	# Retrieve element.
	my $queue_element;
	lives_ok(
		sub
		{
			$queue_element = $queue->next();
		},
		'Retrieve the next element in the queue.',
	);
	isa_ok(
		$queue_element,
		'Queue::DBI::Element',
		'Object returned by next()',
	);
	
	# Lock.
	lives_ok(
		sub
		{
			$queue_element->lock()
			||
			die 'Cannot lock element';
		},
		'Lock element.',
	);
	
	# Requeue.
	lives_ok(
		sub
		{
			$queue_element->requeue()
			||
			die 'Cannot requeue element';
		},
		'Requeue element.',
	);
	
	note( "</Round $try>" );
}

# Now, the seventh time we try to retrieve the element, it should not be returned.
{
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
		'Create the queue object.',
	);
	isa_ok(
		$queue,
		'Queue::DBI',
		'Object returned by Queue::DBI->new()',
	);
	
	# Retrieve element.
	my $queue_element;
	lives_ok(
		sub
		{
			$queue_element = $queue->next();
		},
		'Retrieve the next element in the queue.',
	);
	ok(
		!defined( $queue_element ),
		'No element returned.',
	) || diag( "Queue element returned:\n" . Dumper( $queue_element ) );
}

