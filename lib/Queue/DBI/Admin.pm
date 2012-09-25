package Queue::DBI::Admin;

use warnings;
use strict;

use Carp;
use Data::Dumper;
use Data::Validate::Type;
use Try::Tiny;

use Queue::DBI;


=head1 NAME

Queue::DBI::Admin - Manage Queue::DBI queues.


=head1 VERSION

Version 2.2.1

=cut

our $VERSION = '2.2.1';


=head1 SYNOPSIS

	use Queue::DBI::Admin;
	
	# Create the object which will allow managing the queues.
	my $queues_admin = Queue::DBI::Admin->new(
		database_handle => $dbh,
	);
	
	# Create the tables required by Queue::DBI to store the queues and data.
	$queues_admin->create_tables();
	
	# Create a new queue.
	my $queue = $queues_admin->create_queue( $queue_name );
	
	# Test if a queue exists.
	if ( $queues_admin->has_queue( $queue_name ) )
	{
		...
	}
	
	# Retrieve a queue.
	my $queue = $queues_admin->retrieve_queue( $queue_name );
	
	# Delete a queue.
	$queues_admin->delete_queue( $queue_name );


=head1 SUPPORTED DATABASES

This distribution currently supports:

=over 4

=item * SQLite

=item * MySQL

=back

Please contact me if you need support for another database type, I'm always
glad to add extensions if you can help me with testing.


=head1 METHODS

=head2 new()

Create a new Queue::DBI::Admin object.

	my $queues_admin = Queue::DBI::Admin->new(
		database_handle => $database_handle,
	);

The 'database_handle' parameter is mandatory and must correspond to a
DBI connection handle object.

Optional parameters:

=over 4

=item * 'queues_table_name'

By default, Queue::DBI uses a table named 'queues' to store the queue
definitions. This allows using your own name, if you want to support separate
queuing systems or legacy systems.

=item * 'queue_elements_table_name'

By default, Queue::DBI uses a table named 'queue_elements' to store the queued
data. This allows using your own name, if you want to support separate queuing
systems or legacy systems.

=back

	my $queues_admin = Queue::DBI::Admin->new(
		database_handle           => $database_handle,
		queues_table_name         => $custom_queues_table_name,
		queue_elements_table_name => $custom_queue_elements_table_name,
	);

=cut

sub new
{
	my ( $class, %args ) = @_;
	my $database_handle = delete( $args{'database_handle'} );
	my $queues_table_name = delete( $args{'queues_table_name'} );
	my $queue_elements_table_name = delete( $args{'queue_elements_table_name'} );
	
	croak 'Unrecognized arguments: ' . join( ', ', keys %args )
		if scalar( keys %args ) != 0;
	
	# Verify arguments.
	croak 'The argument "database_handle" must be a DBI connection handle object'
		if !Data::Validate::Type::is_instance( $database_handle, class => 'DBI::db' );
	
	my $self = bless(
		{
			database_handle => $database_handle,
			table_names     =>
			{
				'queues'         => $queues_table_name,
				'queue_elements' => $queue_elements_table_name,
			},
		},
		$class
	);
	
	return $self;
}


=head2 has_tables()

Determine if the tables required for C<Queue::DBI> to operate exist.

In scalar context, this method returns a boolean indicating whether all the
necessary tables exist:

	# Determine if the tables exist.
	my $tables_exist = $queues_admin->has_tables();

In list context, this method returns a boolean indicating whether all the
necessary tables exist, and an arrayref of the name of the missing table(s) if
any:

	# Determine if the tables exist, and the missing one(s).
	my ( $tables_exist, $missing_tables ) = $queues_admin->has_tables();

=cut

sub has_tables
{
	my ( $self ) = @_;
	my $missing_tables = [];
	
	my $database_handle = $self->get_database_handle();
	
	# Check the database type.
	$self->assert_database_type_supported();
	
	# Check if the queues table exists.
	try
	{
		# Disable printing errors out since we expect the statement to fail.
		local $database_handle->{'PrintError'} = 0;
		
		$database_handle->selectrow_array(
			sprintf(
				q|
					SELECT *
					FROM %s
				|,
				$self->get_quoted_queues_table_name(),
			)
		);
	}
	catch
	{
		push( @$missing_tables, $self->get_queues_table_name() );
	};
	
	# Check if the queue elements table exists.
	try
	{
		# Disable printing errors out since we expect the statement to fail.
		local $database_handle->{'PrintError'} = 0;
		
		$database_handle->selectrow_array(
			sprintf(
				q|
					SELECT *
					FROM %s
				|,
				$self->get_quoted_queue_elements_table_name(),
			)
		);
	}
	catch
	{
		push( @$missing_tables, $self->get_queue_elements_table_name() );
	};
	
	my $tables_exist = scalar( @$missing_tables ) == 0 ? 1 : 0;
	return wantarray()
		? ( $tables_exist, $missing_tables )
		: $tables_exist;
}


=head2 create_tables()

Create the tables required by Queue::DBI to store the queues and data.

	$queues_admin->create_tables(
		drop_if_exist => $boolean,
	);

By default, it won't drop any table but you can force that by setting
'drop_if_exist' to 1.

=cut

sub create_tables
{
	my ( $self, %args ) = @_;
	my $drop_if_exist = delete( $args{'drop_if_exist'} ) || 0;
	croak 'Unrecognized arguments: ' . join( ', ', keys %args )
		if scalar( keys %args ) != 0;
	
	# Check the database type.
	my $database_handle = $self->get_database_handle();
	my $database_type = $database_handle->{'Driver'}->{'Name'} || '';
	croak "This database type ($database_type) is not supported yet, please email the maintainer of the module for help"
		if $database_type !~ m/^(?:SQLite|MySQL)$/i;
	
	# Prepare the name of the tables.
	my $queues_table_name = $self->get_queues_table_name();
	my $quoted_queues_table_name = $database_handle->quote_identifier(
		$queues_table_name
	);
	
	my $queue_elements_table_name = $self->get_queue_elements_table_name();
	my $quoted_queue_elements_table_name = $database_handle->quote_identifier(
		$queue_elements_table_name
	);
	
	# Drop the tables, if requested.
	# Note: due to foreign key constraints, we need to drop the tables in the
	# reverse order in which they are created.
	if ( $drop_if_exist )
	{
		$database_handle->do(
			sprintf(
				q|DROP TABLE IF EXISTS %s|,
				$quoted_queue_elements_table_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
		
		$database_handle->do(
			sprintf(
				q|DROP TABLE IF EXISTS %s|,
				$quoted_queues_table_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	}
	
	# Create the list of queues.
	if ( $database_type eq 'SQLite' )
	{
		$database_handle->do(
			sprintf(
				q|
					CREATE TABLE %s
					(
						queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
						name VARCHAR(255) NOT NULL UNIQUE
					)
				|,
				$quoted_queues_table_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	}
	else
	{
		my $unique_index_name = $database_handle->quote_identifier(
			'unq_' . $queues_table_name . '_name',
		);
		
		$database_handle->do(
			sprintf(
				q|
					CREATE TABLE %s
					(
						queue_id INT(11) NOT NULL AUTO_INCREMENT,
						name VARCHAR(255) NOT NULL,
						PRIMARY KEY (queue_id),
						UNIQUE KEY %s (name)
					)
					ENGINE=InnoDB
				|,
				$quoted_queues_table_name,
				$unique_index_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	}
	
	# Create the table that will hold the queue elements.
	if ( $database_type eq 'SQLite' )
	{
		$database_handle->do(
			sprintf(
				q|
					CREATE TABLE %s
					(
						queue_element_id INTEGER PRIMARY KEY AUTOINCREMENT,
						queue_id INTEGER NOT NULL,
						data TEXT,
						lock_time INT(10) DEFAULT NULL,
						requeue_count INT(3) DEFAULT '0',
						created INT(10) NOT NULL DEFAULT '0'
					)
				|,
				$quoted_queue_elements_table_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	}
	else
	{
		my $queue_id_index_name = $database_handle->quote_identifier(
			'idx_' . $queue_elements_table_name . '_queue_id'
		);
		my $queue_id_foreign_key_name = $database_handle->quote_identifier(
			'fk_' . $queue_elements_table_name . '_queue_id'
		);
		
		$database_handle->do(
			sprintf(
				q|
					CREATE TABLE %s
					(
						queue_element_id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
						queue_id INT(11) NOT NULL,
						data TEXT,
						lock_time INT(10) UNSIGNED DEFAULT NULL,
						requeue_count INT(3) UNSIGNED DEFAULT '0',
						created INT(10) UNSIGNED NOT NULL DEFAULT '0',
						PRIMARY KEY (queue_element_id),
						KEY %s (queue_id),
						CONSTRAINT %s FOREIGN KEY (queue_id) REFERENCES %s (queue_id)
					)
					ENGINE=InnoDB
				|,
				$quoted_queue_elements_table_name,
				$queue_id_index_name,
				$queue_id_foreign_key_name,
				$quoted_queues_table_name,
			)
		) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	}
	
	return;
}


=head2 create_queue()

Create a new queue.

	$queues_admin->create_queue( $queue_name );

=cut

sub create_queue
{
	my ( $self, $queue_name ) = @_;
	my $database_handle = $self->get_database_handle();
	
	# Verify parameters.
	croak 'The first parameter must be a queue name'
		if !defined( $queue_name ) || ( $queue_name eq '' );
	
	my $queues_table_name = $database_handle->quote_identifier(
		$self->get_queues_table_name()
	);
	
	# Create the queue.
	$database_handle->do(
		sprintf(
			q|
				INSERT INTO %s ( name )
				VALUES ( ? )
			|,
			$queues_table_name,
		),
		{},
		$queue_name,
	) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	
	return;
}


=head2 has_queue()

Test if a queue exists.

	if ( $queues_admin->has_queue( $queue_name ) )
	{
		...
	}

=cut

sub has_queue
{
	my ( $self, $queue_name ) = @_;
	my $database_handle = $self->get_database_handle();
	
	# Verify parameters.
	croak 'The first parameter must be a queue name'
		if !defined( $queue_name ) || ( $queue_name eq '' );
	
	return try
	{
		my $queue = $self->retrieve_queue( $queue_name );
		
		croak 'The queue does not exist'
			if !defined( $queue );
		
		return 1;
	}
	catch
	{
		return 0;
	};
}


=head2 retrieve_queue()

Retrieve a queue.

	my $queue = $queues_admin->retrieve_queue( $queue_name );

	# See Queue::DBI->new() for all the available options.
	my $queue = $queues_admin->retrieve_queue(
		$queue_name,
		'cleanup_timeout'   => 3600,
		'verbose'           => 1,
		'max_requeue_count' => 5,
	);

=cut

sub retrieve_queue
{
	my ( $self, $queue_name, %args ) = @_;
	my $database_handle = $self->get_database_handle();
	
	# Verify parameters.
	croak 'The first parameter must be a queue name'
		if !defined( $queue_name ) || ( $queue_name eq '' );
	
	# Instantiate a Queue::DBI object.
	my $queue = Queue::DBI->new(
		database_handle           => $database_handle,
		queue_name                => $queue_name,
		queues_table_name         => $self->get_queues_table_name(),
		queue_elements_table_name => $self->get_queue_elements_table_name(),
		%args
	);
	
	return $queue;
}


=head2 delete_queue()

Delete a queue and all associated data, permanently. Use this function at your
own risk!

	$queues_admin->delete_queue( $queue_name );

=cut

sub delete_queue
{
	my ( $self, $queue_name ) = @_;
	my $database_handle = $self->get_database_handle();
	
	# Verify parameters.
	croak 'The first parameter must be a queue name'
		if !defined( $queue_name ) || ( $queue_name eq '' );
	
	# Retrieve the queue object, to get the queue ID.
	my $queue = $self->retrieve_queue( $queue_name );
	
	# Delete queue elements.
	my $queue_elements_table_name = $database_handle->quote_identifier(
		$self->get_queue_elements_table_name()
	);
	
	$database_handle->do(
		sprintf(
			q|
				DELETE
				FROM %s
				WHERE queue_id = ?
			|,
			$queue_elements_table_name,
		),
		{},
		$queue->get_queue_id(),
	) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	
	# Delete the queue.
	my $queues_table_name = $database_handle->quote_identifier(
		$self->get_queues_table_name()
	);
	
	$database_handle->do(
		sprintf(
			q|
				DELETE
				FROM %s
				WHERE queue_id = ?
			|,
			$queues_table_name,
		),
		{},
		$queue->get_queue_id(),
	) || croak 'Cannot execute SQL: ' . $database_handle->errstr();
	
	return;
}


=head1 INTERNAL METHODS

=head2 get_database_handle()

Return the database handle associated with the C<Queue::DBI::Admin>.

	my $database_handle = $queue->get_database_handle();

=cut

sub get_database_handle
{
	my ( $self ) = @_;
	
	return $self->{'database_handle'};
}


=head2 get_queues_table_name()

Return the name of the table used to store queue definitions.

	my $queues_table_name = $queue->get_queues_table_name();

=cut

sub get_queues_table_name
{
	my ( $self ) = @_;
	
	return defined( $self->{'table_names'}->{'queues'} ) && ( $self->{'table_names'}->{'queues'} ne '' )
		? $self->{'table_names'}->{'queues'}
		: $Queue::DBI::DEFAULT_QUEUES_TABLE_NAME;
}


=head2 get_queue_elements_table_name()

Return the name of the table used to store queue elements.

	my $queue_elements_table_name = $queue->get_queue_elements_table_name();

=cut

sub get_queue_elements_table_name
{
	my ( $self ) = @_;
	
	return defined( $self->{'table_names'}->{'queue_elements'} ) && ( $self->{'table_names'}->{'queue_elements'} ne '' )
		? $self->{'table_names'}->{'queue_elements'}
		: $Queue::DBI::DEFAULT_QUEUE_ELEMENTS_TABLE_NAME;
}


=head2 get_quoted_queues_table_name()

Return the name of the table used to store queue definitions, quoted for
inclusion in SQL statements.

	my $quoted_queues_table_name = $queue->get_quoted_queues_table_name();


=cut

sub get_quoted_queues_table_name
{
	my ( $self ) = @_;
	
	my $database_handle = $self->get_database_handle();
	my $queues_table_name = $self->get_queues_table_name();
	
	return defined( $queues_table_name )
		? $database_handle->quote_identifier( $queues_table_name )
		: undef;
}


=head2 sub get_quoted_queue_elements_table_name()

Return the name of the table used to store queue elements, quoted for inclusion
in SQL statements.

	my $quoted_queue_elements_table_name = $queue->get_quoted_queue_elements_table_name();

=cut

sub get_quoted_queue_elements_table_name
{
	my ( $self ) = @_;
	
	my $database_handle = $self->get_database_handle();
	my $queue_elements_table_name = $self->get_queue_elements_table_name();
	
	return defined( $queue_elements_table_name )
		? $database_handle->quote_identifier( $queue_elements_table_name )
		: undef;
}


=head2 assert_database_type_supported()

Assert (i.e., die on failure) whether the database type specified by the
database handle passed to C<new()> is supported or not.

	my $database_type = $queues_admin->assert_database_type_supported();

Note: the type of the database handle associated with the current object is
returned when it is supported.

=cut

sub assert_database_type_supported
{
	my ( $self ) = @_;
	
	# Check the database type.
	my $database_type = $self->get_database_type();
	croak "This database type ($database_type) is not supported yet, please email the maintainer of the module for help"
		if $database_type !~ m/^(?:SQLite|MySQL)$/i;
	
	return $database_type;
}


=head2 get_database_type()

Return the database type corresponding to the database handle associated
with the C<Queue::DBI::Admin> object.

	my $database_type = $queues_admin->get_database_type();

=cut

sub get_database_type
{
	my ( $self ) = @_;
	
	my $database_handle = $self->get_database_handle();
	
	return $database_handle->{'Driver'}->{'Name'} || '';
}


=head1 AUTHOR

Guillaume Aubert, C<< <aubertg at cpan.org> >>.


=head1 BUGS

Please report any bugs or feature requests to C<bug-queue-dbi at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Queue-DBI>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

	perldoc Queue::DBI::Admin


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Queue-DBI>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Queue-DBI>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Queue-DBI>

=item * Search CPAN

L<http://search.cpan.org/dist/Queue-DBI/>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to Sergey Bond for suggesting this administration module to extend
and complete the features offered by C<Queue::DBI>.


=head1 COPYRIGHT & LICENSE

Copyright 2009-2012 Guillaume Aubert.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License version 3 as published by the Free
Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see http://www.gnu.org/licenses/

=cut

1;
