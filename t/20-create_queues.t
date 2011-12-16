#!perl

use strict;
use warnings;

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

# Clean up the tables.
foreach my $table_name ( qw( queues queue_elements ) )
{
	eval
	{
		$dbh->do(
			sprintf(
				q| DELETE FROM %s |,
				$dbh->quote_identifier( $table_name ),
			)
		);
	};
	ok(
		!$@,
		"Empty table >$table_name<.",
	) || diag( "Error >$@<." );
}

# Test creating queues.
foreach my $queue_name ( qw( test1 test2 ) )
{
	eval
	{
		$dbh->do(
			q|
				INSERT INTO queues( queue_id, name )
				VALUES( NULL, ? )
			|,
			{},
			$queue_name,
		);
	};
	ok(
		!$@,
		"Create queue >$queue_name<.",
	) || diag( "Error >$@<." );
}

# Make sure duplicate queue names are handled properly.
eval
{
	# Disable printing errors out since we expect the test to fail.
	local $dbh->{'PrintError'} = 0;
	
	$dbh->do(
		q|
			INSERT INTO queues( queue_id, name )
			VALUES( NULL, ? )
		|,
		{},
		'test1',
	);
};
ok(
	$@,
	"Reject duplicate queue name.",
) || diag( "Error >$@<." );
