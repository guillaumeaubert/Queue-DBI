#!perl

use strict;
use warnings;

use Test::More tests => 34;

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
for ( my $i = 0; $i < 5; $i++ )
{
	my $data =
	{
		'count' => $i,
	};
	
	eval
	{
		$queue->enqueue( $data );
	};
	
	ok(
		!$@,
		"Queue data - Element $i",
	) || diag( "Could not enqueue the following data:\n" . Dumper( $data ) );
};

# Retrieve data.
for ( my $i = 0; $i < 5; $i++ )
{
	note( "<Retrieving element $i>" );
	
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
		'Queue::DBI::Element object returned',
	) || diag( '$queue: ' . ( defined( $queue_element ) ? 'ref() >' . ref( $queue_element ) .'<' : 'undef' ) );
	
	my $data;
	eval
	{
		$data = $queue_element->data();
	};
	ok(
		!$@,
		'Extract data',
	) || diag( "Error: $@ " );
	ok(
		defined( $data ),
		'Data defined',
	);
	ok(
		defined( $data->{'count'} ) && ( $data->{'count'} == $i ),
		'Find expected item',
	) || diag( "Data:\n" . Dumper( $data ) );
	
	note( "</Retrieving element $i>" );
}
