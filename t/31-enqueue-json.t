#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::FailWarnings;
use Test::More tests => 7;

use lib 't/';
use LocalTest;

use Queue::DBI;
use JSON::XS;


my $dbh = LocalTest::ok_database_handle();
my $JSON = JSON::XS->new;

# Instantiate the queue object.
my $queue;
lives_ok(
	sub
	{
		$queue = Queue::DBI->new(
			'queue_name'      => 'test1',
			'database_handle' => $dbh,
			'cleanup_timeout' => 3600,
			'verbose'         => 0,
			'serialize_freeze'  => sub { $JSON->encode($_[0]) },
			'serialize_thaw'    => sub { $JSON->decode($_[0]) },
		);
	},
	'Instantiate a new Queue::DBI object.',
);

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

# Retrieve data.
my $queue_element;
lives_ok(
	sub
	{
		$queue_element = $queue->next();
	},
	'Call to retrieve the next item in the queue.',
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

# Remove.
lives_ok(
	sub
	{
		$queue_element->success()
		||
		die 'Cannot mark as successfully processed';
	},
	'Mark as successfully processed.',
);

