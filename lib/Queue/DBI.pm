package Queue::DBI;

use warnings;
use strict;

use Data::Dumper;
use Carp;
use Storable qw();
use MIME::Base64 qw();

use Queue::DBI::Element;


=head1 NAME

Queue::DBI - A queueing module with an emphasis on safety, using DBI as a
storage system for queued data.


=head1 VERSION

Version 1.7.1

=cut

our $VERSION = '1.7.1';


our $UNLIMITED_RETRIES = -1;

our $DEFAULT_QUEUES_TABLE_NAME = 'queues';

our $DEFAULT_QUEUE_ELEMENTS_TABLE_NAME = 'queue_elements';

our $MAX_VALUE_SIZE = 65535;


=head1 SYNOPSIS

This module allows you to safely use a queueing system by preventing
backtracking, infinite loops and data loss.

An emphasis of this distribution is to provide an extremely reliable dequeueing
mechanism without having to use transactions.

	use Queue::DBI;
	my $queue = Queue::DBI->new(
		'queue_name'      => $queue_name,
		'database_handle' => $dbh,
		'cleanup_timeout' => 3600,
		'verbose'         => 1,
	);
	
	$queue->enqueue( $data );
	
	while ( my $queue_element = $queue->next() )
	{
		next
			unless $queue_element->lock();
		
		eval {
			# Do some work
			process( $queue_element->{'email'} );
		};
		if ( $@ )
		{
			# Something failed, we clear the lock but don't delete the record in the
			# queue so that we can try again next time
			$queue_element->requeue();
		}
		else
		{
			# All good, remove definitively the element
			$queue_element->success();
		}
	}
	
	# Requeue items that have been locked for more than 6 hours
	$queue->cleanup( 6 * 3600 );


=head1 METHODS

=head2 new()

Create a new Queue::DBI object.

	my $queue = Queue::DBI->new(
		'queue_name'        => $queue_name,
		'database_handle'   => $dbh,
		'cleanup_timeout'   => 3600,
		'verbose'           => 1,
		'max_requeue_count' => 5,
	);
	
	# Custom table names (optional).
	my $queue = Queue::DBI->new(
		'queue_name'                => $queue_name,
		'database_handle'           => $dbh,
		'cleanup_timeout'           => 3600,
		'verbose'                   => 1,
		'max_requeue_count'         => 5,
		'queues_table_name'         => $custom_queues_table_name,
		'queue_elements_table_name' => $custom_queue_elements_table_name,
	);

Parameters:

=over 4

=item * 'queue_name'

Mandatory, the name of the queue elements will be added to / removed from.

=item * 'database handle'

Mandatory, a DBI / DBD::Mysql object.

=item * 'cleanup_timeout'

Optional, if set to an integer representing a time in seconds, the module will
automatically make available again elements that have been locked longuer than
that time.

=item * 'verbose'

Optional, see verbose() for options.

=item * 'max_requeue_count'

By default, Queue:::DBI will retrieve again the queue elements that were
requeued without limit to the number of times they have been requeued. Use this
option to specify how many times an element can be requeued before it is
ignored when retrieving elements.

=item * 'queues_table_name'

By default, Queue::DBI uses a table named 'queues' to store the queue
definitions. This allows using your own name, if you want to support separate
queuing systems or legacy systems.

=item * 'queue_elements_table_name'

By default, Queue::DBI uses a table named 'queue_elements' to store the queued
data. This allows using your own name, if you want to support separate queuing
systems or legacy systems.

=back

=cut

sub new
{
	my ( $class, %args ) = @_;
	
	# Check parameters.
	foreach my $arg ( qw( queue_name database_handle ) )
	{
		die "Argument '$arg' is needed to create the Queue::DBI object"
			unless defined( $args{$arg} ) && ( $args{$arg} ne '' );
	}
	die 'Cleanup timeout must be an integer representing seconds'
		if defined( $args{'cleanup_timeout'} ) && ( $args{'cleanup_timeout'} !~ m/^\d+$/ );
	
	# Create the object.
	my $dbh = $args{'database_handle'};
	my $self = bless(
		{
			'dbh'         => $dbh,
			'queue_name'  => $args{'queue_name'},
			'table_names' =>
			{
				'queues'         => $args{'queues_table_name'},
				'queue_elements' => $args{'queue_elements_table_name'},
			},
			'verbose'     => 0,
		},
		$class
	);
	
	# Find the queue id.
	my @queue = $dbh->selectrow_array(
		sprintf(
			q|
				SELECT queue_id
				FROM %s
				WHERE name = ?
			|,
			$dbh->quote_identifier( $self->get_queues_table_name() ),
		),
		{},
		$args{'queue_name'},
	);
	die "The queue >$args{'queue_name'}< doesn't exist in the lookup table."
		unless defined( $queue[0] ) && ( $queue[0] =~ m/^\d+$/ );
	$self->{'queue_id'} = $queue[0];
	
	$self->verbose( $args{'verbose'} )
		if defined( $args{'verbose'} );
	
	$self->max_requeue_count(
		defined( $args{'max_requeue_count'} )
			? $args{'max_requeue_count'}
			: $Queue::DBI::UNLIMITED_RETRIES
	);
	
	$self->cleanup( $args{'cleanup_timeout'} )
		if defined( $args{'cleanup_timeout'} );
	
	return $self;
}


=head2 verbose()

Control the verbosity of the warnings in the code.

	$queue->verbose(1); # turn on verbose information
	
	$queue->verbose(2); # be extra verbose
	
	$queue->verbose(0); # quiet now!
	
	warn 'Verbose' if $queue->verbose(); # getter-style
	
	warn 'Very verbose' if $queue->verbose() > 1;
	
0 will not display any warning, 1 will only give one line warnings about the
current operation and 2 will also usually output the SQL queries performed.

=cut

sub verbose
{
	my ( $self, $verbose ) = @_;
	
	$self->{'verbose'} = ( $verbose || 0 )
		if defined( $verbose );
	
	return $self->{'verbose'};
}


=head2 max_requeue_count()

Sets the number of time an element can be requeued before it is ignored when
retrieving elements. Set it to $Queue::DBI::UNLIMITED_RETRIES to reset
Queue::DBI back to its default behavior of re-pulling elements without limit.

	# Don't keep pulling the element if it has been requeued more than 5 times.
	$queue->max_requeue_count( 5 );
	
	# Keep pulling elements regardless of the number of times they have been
	# requeued.
	$queue->max_requeue_count( $UNLIMITED_RETRIES );
	
	# Find how many times a queue object will try to requeue.
	my $max_requeue_count = $queue->max_requeue_count();

=cut

sub max_requeue_count
{
	my ( $self, $max_requeue_count ) = @_;
	
	if ( defined( $max_requeue_count ) )
	{
		if ( ( $max_requeue_count =~ m/^\d+$/ ) || ( $max_requeue_count eq $UNLIMITED_RETRIES ) )
		{
			$self->{'max_requeue_count'} = $max_requeue_count;
		}
		else
		{
			die 'max_requeue_count must be an integer or $Queue::DBI::UNLIMITED_RETRIES';
		}
	}
	
	return $self->{'max_requeue_count'};
}


=head2 get_queue_id()

Returns the queue ID corresponding to the current queue object.

	my $queue_id = $self->get_queue_id();

=cut

sub get_queue_id
{
	my ( $self ) = @_;
	
	return $self->{'queue_id'};
}


=head2 count()

Returns the number of elements in the queue.

=cut

sub count
{
	my ( $self ) = @_;
	my $verbose = $self->verbose();
	my $dbh = $self->get_dbh();
	carp "Entering count()." if $verbose;
	
	my $data = $dbh->selectrow_arrayref(
		sprintf(
			q|
				SELECT COUNT(*)
				FROM %s
				WHERE queue_id = ?
			|,
			$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
		),
		{},
		$self->get_queue_id(),
	) || die 'Cannot execute SQL: ' . $dbh->errstr;
	
	my $element_count = defined( $data ) && defined( $data->[0] ) ? $data->[0] : 0;
	
	carp "Found $element_count elements, leaving count()." if $verbose;
	
	return $element_count;
}


=head2 enqueue()

Adds a new element at the end of the current queue.

	my $queue_element_id = $queue->enqueue( $data );

The data passed can be a scalar or a reference to a complex data
structure. There is no limitation on the type of data that can be stored
as it is serialized for storage in the database.

=cut

sub enqueue
{
	my ( $self, $data ) = @_;
	my $verbose = $self->verbose();
	my $dbh = $self->get_dbh();
	carp "Entering enqueue()." if $verbose;
	carp "Data is: " . Dumper( $data ) if $verbose > 1;
	
	my $encoded_data = MIME::Base64::encode_base64( Storable::freeze( $data ) );
	die 'The size of the data to store exceeds the maximum internal storage size available.'
		if length( $encoded_data ) > $MAX_VALUE_SIZE;
	
	$dbh->do(
		sprintf(
			q|
				INSERT INTO %s( queue_id, data, created )
				VALUES ( ?, ?, ? )
			|,
			$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
		),
		{},
		$self->get_queue_id(),
		$encoded_data,
		time(),
	) || die 'Cannot execute SQL: ' . $dbh->errstr();
	
	# We need to reset the internal cached value preventing infinite loops, other-
	# wise this new element will not be taken into account by the current queue
	# object.
	$self->{'max_id'} = undef;
	
	carp "Element inserted, leaving enqueue()." if $verbose;
	
	return $dbh->{'mysql_insertid'};
}


=head2 next()

Retrieves the next element from the queue and returns it in the form of a
Queue::DBI::Element object.

	my $queue_element = $queue->next();
	
	while ( my $queue_element = $queue->next() )
	{
		# [...]
	}

Additionally, for testing purposes, a list of IDs to use when trying to retrieve
elements can be specified using 'search_in_ids':

	my $queue_item = $queue->next( 'search_in_ids' => [ 123, 124, 125 ] );

=cut

sub next
{
	my ( $self, %args ) = @_;
	my $verbose = $self->verbose();
	carp "Entering next()." if $verbose;
	
	my $elements = $self->retrieve_batch(
		1,
		'search_in_ids' => defined( $args{'search_in_ids'} )
			? $args{'search_in_ids'}
			: undef,
	);
	
	my $return = defined( $elements ) && ( scalar( @$elements ) != 0 )
		? $elements->[0]
		: undef;
	
	carp "Leaving next()." if $verbose;
	return $return;
}


=head2 retrieve_batch()

Retrieves a batch of elements from the queue and returns them in an arrayref.

This method requires an integer to be passed as parameter to indicate the
maximum size of the batch to be retrieved.

	my $queue_elements = $queue->retrieve_batch( 500 );
	
	foreach ( @$queue_elements )
	{
		# [...]
	}

Additionally, for testing purposes, a list of IDs to use when trying to retrieve
elements can be specified using 'search_in_ids':

	my $queue_items = $queue->retrieve_batch(
		10,
		'search_in_ids' => [ 123, 124, 125 ],
	);

=cut

sub retrieve_batch
{
	my ( $self, $number_of_elements_to_retrieve, %args ) = @_;
	my $verbose = $self->verbose();
	my $dbh = $self->get_dbh();
	carp "Entering retrieve_batch()." if $verbose;
	
	# Check parameters
	die 'The number of elements to retrieve from the queue is not properly formatted'
		unless defined( $number_of_elements_to_retrieve ) && ( $number_of_elements_to_retrieve =~ m/^\d+$/ );
	
	# Prevent infinite loops
	unless ( defined( $self->{'max_id'} ) )
	{
		my $data = $dbh->selectrow_arrayref(
			sprintf(
				q|
					SELECT MAX(queue_element_id)
					FROM %s
					WHERE queue_id = ?
				|,
				$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
			),
			{},
			$self->get_queue_id(),
		);
		die 'Cannot execute SQL: ' . $dbh->errstr() if defined( $dbh->errstr() );
		if ( defined( $data ) && defined( $data->[0] ) )
		{
			$self->{'max_id'} = $data->[0];
		}
		else
		{
			# Empty queue
			carp "Detected empty queue, leaving." if $verbose;
			return;
		}
	}
	
	# Prevent backtracking in case elements are requeued
	$self->{'last_id'} = -1
		unless defined( $self->{'last_id'} );
	
	# Detect end of queue quicker
	if ( $self->{'last_id'} == $self->{'max_id'} )
	{
		carp "Finished processing queue, leaving." if $verbose;
		return [];
	}
	
	# Make sure we don't use requeued elements more times than specified.
	my $max_requeue_count = $self->max_requeue_count();
	my $sql_max_requeue_count = defined( $max_requeue_count ) && ( $max_requeue_count != $UNLIMITED_RETRIES )
		? 'AND requeue_count <= ' . $dbh->quote( $max_requeue_count )
		: '';
	
	# Retrieve the first available elements from the queue
	carp "Retrieving data." if $verbose;
	carp "Parameters:\n\tLast ID: $self->{'last_id'}\n\tMax ID: $self->{'max_id'}\n" if $verbose > 1;
	my $ids = defined( $args{'search_in_ids'} )
		? 'AND queue_element_id IN (' . join( ',', map { $dbh->quote( $_ ) } @{ $args{'search_in_ids' } } ) . ')'
		: '';
	my $data = $dbh->selectall_arrayref(
		sprintf(
			q|
				SELECT queue_element_id, data, requeue_count
				FROM %s
				WHERE queue_id = ?
					AND lock_time IS NULL
					AND queue_element_id >= ?
					AND queue_element_id <= ?
					%s
					%s
				ORDER BY queue_element_id ASC
				LIMIT ?
			|,
			$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
			$ids,
			$sql_max_requeue_count,
		),
		{},
		$self->get_queue_id(),
		$self->{'last_id'} + 1,
		$self->{'max_id'},
		$number_of_elements_to_retrieve,
	);
	die 'Cannot execute SQL: ' . $dbh->errstr() if defined( $dbh->errstr() );
	
	# All the remaining elements are locked
	return []
		unless defined( $data ) && ( scalar( @$data) != 0 );
	
	# Create objects
	carp "Creating new Queue::DBI::Element objects." if $verbose;
	my @return = ();
	foreach my $row ( @$data )
	{
		push(
			@return,
			Queue::DBI::Element->new(
				'queue'         => $self,
				'data'          => Storable::thaw( MIME::Base64::decode_base64( $row->[1] ) ),
				'id'            => $row->[0],
				'requeue_count' => $row->[2],
			)
		);
	}
	
	# Prevent backtracking in case elements are requeued
	$self->{'last_id'} = $return[-1]->id();
	
	carp "Leaving retrieve_batch()." if $verbose;
	return \@return;
}


=head2 get_element_by_id()

Retrieves a queue element using a queue element ID, ignoring any lock placed on
that element.

This method is mostly useful when doing a lock on an element and then calling
success/requeue asynchroneously.

This method requires a queue element ID to be passed as parameter.

	my $queue_element = $queue->get_element_by_id( 123456 );

=cut

sub get_element_by_id
{
	my ( $self, $queue_element_id ) = @_;
	my $verbose = $self->verbose();
	my $dbh = $self->get_dbh();
	carp "Entering get_element_by_id()." if $verbose;
	
	# Check parameters.
	die 'A queue element ID is required by this method'
		unless defined( $queue_element_id );
	
	# Retrieve the specified element from the queue.
	carp "Retrieving data." if $verbose;
	my $data = $dbh->selectrow_hashref(
		sprintf(
			q|
				SELECT *
				FROM %s
				WHERE queue_id = ?
					AND queue_element_id = ?
			|,
			$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
		),
		{},
		$self->get_queue_id(),
		$queue_element_id,
	);
	die 'Cannot execute SQL: ' . $dbh->errstr() if defined( $dbh->errstr() );
	
	# Queue element ID doesn't exist or belongs to another queue.
	return unless defined( $data );
	
	# Create the Queue::DBI::Element object.
	carp "Creating a new Queue::DBI::Element object." if $verbose;
	
	my $queue_element = Queue::DBI::Element->new(
		'queue'         => $self,
		'data'          => Storable::thaw( MIME::Base64::decode_base64( $data->{'data'} ) ),
		'id'            => $data->{'queue_element_id'},
		'requeue_count' => $data->{'requeue_count'},
	);
	
	carp "Leaving get_element_by_id()." if $verbose;
	return $queue_element;
}


=head2 cleanup()

Requeue items that have been locked for more than the time in seconds specified
as parameter.

Returns the items requeued so that a specific action can be taken on them.

	my $elements = $queue->cleanup( $time_in_seconds );
	foreach my $element ( @$elements )
	{
		# $element is a Queue::DBI::Element object
	}

=cut

sub cleanup
{
	my ( $self, $time_in_seconds ) = @_;
	my $verbose = $self->verbose();
	my $dbh = $self->get_dbh();
	carp "Entering cleanup()." if $verbose;
	
	$time_in_seconds ||= '';
	die 'Time in seconds is not correctly formatted'
		unless $time_in_seconds =~ m/^\d+$/;
	
	# Find all the orphans
	carp "Retrieving data." if $verbose;
	my $rows = $dbh->selectall_arrayref(
		sprintf(
			q|
				SELECT queue_element_id, data, requeue_count
				FROM %s
				WHERE queue_id = ?
					AND lock_time < ?
			|,
			$dbh->quote_identifier( $self->get_queue_elements_table_name() ),
		),
		{},
		$self->get_queue_id(),
		time() - $time_in_seconds,
	);
	die 'Cannot execute SQL: ' . $dbh->errstr() if defined( $dbh->errstr() );
	return []
		unless defined( $rows );
	
	# Create objects and requeue them
	carp "Creating new Queue::DBI::Element objects." if $verbose;
	my $queue_elements = [];
	foreach my $row ( @$rows )
	{
		my $queue_element = Queue::DBI::Element->new(
			'queue'         => $self,
			'data'          => Storable::thaw( MIME::Base64::decode_base64( $row->[1] ) ),
			'id'            => $row->[0],
			'requeue_count' => $row->[2],
		);
		# If this item was requeued by another process since its
		# being SELECTed a moment ago, requeue() will return failure
		# and this process will ignore it.
		push( @$queue_elements, $queue_element )
			if $queue_element->requeue();
	}
	carp "Found " . scalar( @$queue_elements ) . " orphaned element(s)." if $verbose;
	
	carp "Leaving cleanup()." if $verbose;
	return $queue_elements;
}


=head2 create_tables()

Creates the tables in the database the database handle passed as parameter
points to. This allows setting up Queue::DBI's underlying database structure
quickly.

	Queue::DBI::create_tables(
		dbh           => $dbh,
		drop_if_exist => $boolean,
		sqlite        => $boolean,
	);

By default, it won't drop any table but you can force that by setting
'drop_if_exist' to 1. 'sqlite' is also set to 0 by default, as this parameter
is used only for testing.

=cut

sub create_tables
{
	my ( %args ) = @_;
	
	# Check arguments.
	die 'Missing database handle'
		unless defined( $args{'dbh'} );
	
	foreach my $arg ( qw( drop_if_exist sqlite ) )
	{
		$args{$arg} = 0
			unless defined( $args{$arg} ) && $args{$arg};
	}
	
	# Create the list of queues.
	$args{'dbh'}->do( q|DROP TABLE IF EXISTS `queues`| )
		if $args{'drop_if_exist'};
	$args{'dbh'}->do(
		$args{'sqlite'}
		? q|
			CREATE TABLE `queues`
			(
				`queue_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				`name` VARCHAR(255) NOT NULL UNIQUE
			)
		|
		: q|
			CREATE TABLE `queues`
			(
				`queue_id` INT(11) NOT NULL AUTO_INCREMENT,
				`name` VARCHAR(255) NOT NULL,
				PRIMARY KEY (`queue_id`),
				UNIQUE KEY `name` (`name`)
			)
			ENGINE=InnoDB
		|
	);
	
	# Create the table that will hold the queue elements.
	$args{'dbh'}->do( q|DROP TABLE IF EXISTS `queue_elements`| )
		if $args{'drop_if_exist'};
	$args{'dbh'}->do(
		$args{'sqlite'}
		? q|
			CREATE TABLE `queue_elements`
			(
				`queue_element_id` INTEGER PRIMARY KEY AUTOINCREMENT,
				`queue_id` INTEGER NOT NULL,
				`data` TEXT,
				`lock_time` INT(10) DEFAULT NULL,
				`requeue_count` INT(3) DEFAULT '0',
				`created` INT(10) NOT NULL DEFAULT '0'
			)
		|
		: q|
			CREATE TABLE `queue_elements`
			(
				`queue_element_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
				`queue_id` INT(11) NOT NULL,
				`data` TEXT,
				`lock_time` INT(10) UNSIGNED DEFAULT NULL,
				`requeue_count` INT(3) UNSIGNED DEFAULT '0',
				`created` INT(10) UNSIGNED NOT NULL DEFAULT '0',
				PRIMARY KEY (`queue_element_id`),
				KEY `idx_fk_queue_id` (`queue_id`),
				CONSTRAINT `queue_element_ibfk_1` FOREIGN KEY (`queue_id`) REFERENCES `queue` (`queue_id`)
			)
			ENGINE=InnoDB
		|
	);
	
	return 1;
}


=head1 INTERNAL METHODS

=head2 get_dbh()

Returns the database handle used for this queue.

	my $dbh = $queue->get_dbh();

=cut

sub get_dbh
{
	my ( $self ) = @_;
	
	return $self->{'dbh'};
}


=head2 get_queues_table_name()

Returns the name of the table used to store queue definitions.

	my $queues_table_name = $queue->get_queues_table_name();

=cut

sub get_queues_table_name
{
	my ( $self ) = @_;
	
	return defined( $self->{'table_names'}->{'queues'} ) && ( $self->{'table_names'}->{'queues'} ne '' )
		? $self->{'table_names'}->{'queues'}
		: $DEFAULT_QUEUES_TABLE_NAME;
}


=head2 get_queue_elements_table_name()

Returns the name of the table used to store queue definitions.

	my $queue_elements_table_name = $queue->get_queue_elements_table_name();

=cut

sub get_queue_elements_table_name
{
	my ( $self ) = @_;
	
	return defined( $self->{'table_names'}->{'queue_elements'} ) && ( $self->{'table_names'}->{'queue_elements'} ne '' )
		? $self->{'table_names'}->{'queue_elements'}
		: $DEFAULT_QUEUE_ELEMENTS_TABLE_NAME;
}


=head1 AUTHOR

Guillaume Aubert, C<< <aubertg at cpan.org> >>.


=head1 BUGS

Please report any bugs or feature requests to C<bug-queue-dbi at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Queue-DBI>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

	perldoc Queue::DBI


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

Thanks to ThinkGeek (L<http://www.thinkgeek.com/>) and its corporate overlords
at Geeknet (L<http://www.geek.net/>), for footing the bill while I eat pizza
and write code for them!

Thanks to Jacob Rose C<< <jacob at thinkgeek.com> >>, who wrote the first
queueing module at ThinkGeek L<http://www.thinkgeek.com> and whose work
provided the inspiration to write this full-fledged queueing system. His
contribution to shaping the original API in version 1.0.0 was also very
valuable.

Thanks to Jamie McCarthy for the locking mechanism improvements in version 1.1.0.


=head1 COPYRIGHT & LICENSE

Copyright 2009-2012 Guillaume Aubert.

This program is free software; you can redistribute it and/or modify it
under the terms of the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
