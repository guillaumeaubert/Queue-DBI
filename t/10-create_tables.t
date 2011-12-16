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
	'Create connection to a SQLite database',
);

eval
{
	# Disable printing errors out since we expect the test to fail.
	local $dbh->{'PrintError'} = 0;
	
	$dbh->selectrow_array( q| SELECT * FROM queues | );
};
ok(
	$@,
	'The queues table does not exist yet.',
) || diag( "Error >$@<." );

eval
{
	# Disable printing errors out since we expect the test to fail.
	local $dbh->{'PrintError'} = 0;
	
	$dbh->selectrow_array( q| SELECT * FROM queue_elements | );
};
ok(
	$@,
	'The queue elements table does not exist yet.',
) || diag( "Error >$@<." );

eval
{
	Queue::DBI::create_tables(
		dbh           => $dbh,
		drop_if_exist => 1,
		sqlite        => 1,
	);
};
ok(
	!$@,
	'Create tables',
) || diag( "Error >$@<." );

eval
{
	$dbh->selectrow_array( q| SELECT * FROM queues | );
};
ok(
	!$@,
	'The queues table exists.',
) || diag( "Error >$@<." );

eval
{
	$dbh->selectrow_array( q| SELECT * FROM queue_elements | );
};
ok(
	!$@,
	'The queue elements table exists.',
) || diag( "Error >$@<." );
