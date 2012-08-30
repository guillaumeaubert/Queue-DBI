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
	'delete_queue',
);

subtest(
	'Test using default tables.',
	sub
	{
		test_retrieve_queue(
			new_args   => {},
			queue_name => 'test_queue',
		);
	}
);

subtest(
	'Test using custom tables.',
	sub
	{
		test_retrieve_queue(
			new_args   =>
			{
				'queues_table_name'         => 'test_queues',
				'queue_elements_table_name' => 'test_queue_elements',
			},
			queue_name => 'test_queue_custom',
		);
	}
);


sub test_retrieve_queue
{
	my ( %args ) = @_;
	my $new_args = delete( $args{'new_args'} ) || {};
	my $queue_name = delete( $args{'queue_name'} );
	
	die 'The queue name must be specified'
		if !defined( $queue_name ) || ( $queue_name eq '' );
	
	plan( tests => 2 );
	
	my $queue_admin;
	lives_ok(
		sub
		{
			$queue_admin = Queue::DBI::Admin->new(
				'database_handle' => $dbh,
				%$new_args,
			);
		},
		'Instantiate a new Queue::DBI::Admin object.',
	);
	
	lives_ok(
		sub
		{
			$queue_admin->delete_queue(
				$queue_name,
			);
		},
		"Delete queue >$queue_name<.",
	);
}