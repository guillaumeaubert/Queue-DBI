package Queue::DBI::Element;

use warnings;
use strict;

use Data::Dumper;
use Carp;

=head1 NAME

Queue::DBI::Element - An object representing an element pulled from the queue

=head1 VERSION

Version 1.7.1

=cut

our $VERSION = '1.7.1';


=head1 SYNOPSIS

Please refer to the documentation for Queue::DBI.

=head1 METHODS

=head2 new()

Create a new Queue::DBI::Element object.

	my $element = Queue::DBI::Element->new(
		'queue'         => $queue,
		'data'          => $data,
		'id'            => $id,
		'requeue_count' => $requeue_count,
	);

All parameters are mandatory and correspond respectively to the Queue::DBI
object used to pull the element's data, the data, the ID of the element
in the database and the number of times the element has been requeued before.

It is not recommended for direct use. You should be using the following to get
Queue::DBI::Element objects:

	my $queue = $queue->next();

=cut

sub new
{
	my ( $class, %args ) = @_;
	
	# Check parameters
	foreach my $arg ( qw( data id requeue_count ) )
	{
		croak "Argument '$arg' is needed to create the Queue::DBI object"
			if !defined( $args{$arg} ) || ( $args{$arg} eq '' );
	}
	croak 'Pass a Queue::DBI object to create an Queue::DBI::Element object'
		unless defined( $args{'queue'} ) && $args{'queue'}->isa( 'Queue::DBI' );
	
	# Create the object
	my $self = bless(
		{
			'queue'         => $args{'queue'},
			'data'          => $args{'data'},
			'id'            => $args{'id'},
			'requeue_count' => $args{'requeue_count'},
		},
		$class
	);
	
	return $self;
}


=head2 lock()

Locks the element so that another process acting on the queue cannot get a hold
of it

	if ( $element->lock() )
	{
		print "Element successfully locked.\n";
	}
	else
	{
		print "The element has already been removed or locked.\n";
	}

=cut

sub lock ## no critic (Subroutines::ProhibitBuiltinHomonyms)
{
	my ( $self ) = @_;
	my $queue = $self->queue();
	my $verbose = $queue->verbose();
	my $dbh = $queue->get_dbh();
	carp "Entering lock()." if $verbose;
	
	my $rows = $dbh->do(
		sprintf(
			q|
				UPDATE %s
				SET lock_time = ?
				WHERE queue_element_id = ?
					AND lock_time IS NULL
			|,
			$dbh->quote_identifier( $queue->get_queue_elements_table_name() ),
		),
		{},
		time(),
		$self->id(),
	) || croak 'Cannot lock element: ' . $dbh->errstr;
	
	my $success = ( defined( $rows ) && ( $rows == 1 ) ) ? 1 : 0;
	carp "Element locked: " . ( $success ? 'success' : 'already locked or gone' ) . "." if $verbose;
	
	carp "Leaving lock()." if $verbose;
	return $success;
}


=head2 requeue()

In case the processing of an element has failed

	if ( $element->requeue() )
	{
		print "Element successfully requeued.\n";
	}
	else
	{
		print "The element has already been removed or been requeued.\n";
	}

=cut

sub requeue
{
	my ( $self ) = @_;
	my $queue = $self->queue();
	my $verbose = $queue->verbose();
	my $dbh = $queue->get_dbh();
	carp "Entering requeue()." if $verbose;
	
	my $rows = $dbh->do(
		sprintf(
			q|
				UPDATE %s
				SET
					lock_time = NULL,
					requeue_count = requeue_count + 1
				WHERE queue_element_id = ?
					AND lock_time IS NOT NULL
			|,
			$dbh->quote_identifier( $queue->get_queue_elements_table_name() ),
		),
		{},
		$self->id(),
	);
	
	# Since Queue::DBI does not enclose the SELECTing of a queue_element
	# to be requeued, and this actual requeueing, it is possible for the
	# element to be requeued by another process in-between. It may even
	# be requeued, relocked, and successfully removed in-between. In either
	# case, the number of rows affected would be 0, and do() would return
	# 0E0, perl's "0 but true" value. This is not an error. However, if
	# -1 or undef is returned, DBI.pm encountered some sort of error.
	if ( ! defined( $rows ) || $rows == -1 )
	{
		# Always carp the information, since it is an error that
		# most likely doesn't come from this module.
		my $error = $dbh->errstr();
		carp 'Cannot requeue element: ' . ( defined( $error ) ? $error : 'no error returned by DBI' );
		return 0;
	}
	
	my $requeued = ( $rows == 1 ) ? 1 : 0;
	carp "Element requeued: " . ( $requeued ? 'done' : 'already requeued or gone' ) . "." if $verbose;
	
	# Update the requeue_count on the object as well if the database update was
	# successful.
	$self->{'requeue_count'}++
		if $requeued;
	
	carp "Leaving requeue()." if $verbose;
	return $requeued;
}


=head2 success()

Removes the element from the queue after its processing has successfully been
completed.

	if ( $element->success() )
	{
		print "Element successfully removed from queue.\n";
	}
	else
	{
		print "The element has already been removed.\n";
	}

=cut

sub success
{
	my ( $self ) = @_;
	my $queue = $self->queue();
	my $verbose = $queue->verbose();
	my $dbh = $queue->get_dbh();
	carp "Entering success()." if $verbose;
	
	# Possible improvement:
	# Add $self->{'lock_time'} in lock() and insist that it matches that value
	# when trying to delete the element here.
	
	# First, we try to delete the LOCKED element.
	my $rows = $dbh->do(
		sprintf(
			q|
				DELETE
				FROM %s
				WHERE queue_element_id = ?
					AND lock_time IS NOT NULL
			|,
			$dbh->quote_identifier( $queue->get_queue_elements_table_name() ),
		),
		{},
		$self->id(),
	);
	
	if ( ! defined( $rows ) || $rows == -1 )
	{
		croak 'Cannot remove element: ' . $dbh->errstr();
	}
	
	my $success = 0;
	if ( $rows == 1 )
	{
		# A LOCKED element was found and deleted, this is a success.
		carp "Found a LOCKED element and deleted it. Element successfully processed." if $verbose;
		$success = 1;
	}
	else
	{
		# No LOCKED element found to delete, try to find an UNLOCKED one in case it
		# got requeued by a parallel process.
		my $rows = $dbh->do(
			sprintf(
				q|
					DELETE
					FROM %s
					WHERE queue_element_id = ?
				|,
				$dbh->quote_identifier( $self->get_queues_table_name() ),
			),
			{},
			$self->id(),
		);
		
		if ( ! defined( $rows ) || $rows == -1 )
		{
			croak 'Cannot remove element: ' . $dbh->errstr;
		}
		
		if ( $rows == 1 )
		{
			# An UNLOCKED element was found and deleted. It probably means that
			# another process is still working on that element as well (possibly
			# because this element's lock timed-out, got cleaned up and picked by
			# another process).
			# Always carp for those, technically we processed the element successfully
			# so deleting it is the correct step to take, but we still want to throw
			# some warning for the user.
			carp 'Another process is probably working on the same element, as it was found UNLOCKED when we deleted it. '
				. 'Check parallelization issues in your code!';
			$success = 1;
		}
		else
		{
			# No element found at all. It probably means that another process had been
			# working on that element, but completed successfully its run and deleted
			# it.
			carp 'Another process has probably worked on this element and already deleted it after completing its operations. '
				. 'Check parallelization issues in your code!' if $verbose;
			$success = 0;
		}
	}
	
	carp "Leaving success()." if $verbose;
	return $success;
}


=head2 data()

Returns the data initially queued.

	my $data = $element->data();

=cut

sub data
{
	my ( $self ) = @_;
	
	return $self->{'data'};
}


=head2 requeue_count()

Returns the number of times that the current element has been requeued.

=cut

sub requeue_count
{
	my ( $self ) = @_;
	
	return $self->{'requeue_count'};
}

=head2 id()

Returns the ID of the current element

	my $id = $element->id();

=cut

sub id
{
	my ( $self ) = @_;
	
	return $self->{'id'};
}


=head1 INTERNAL METHODS

=head2 queue()

Returns the Queue::DBI object used to pull the current element.

=cut

sub queue
{
	my ( $self ) = @_;
	
	return $self->{'queue'};
}


=head1 AUTHOR

Guillaume Aubert, C<< <aubertg at cpan.org> >>.


=head1 BUGS

Please report any bugs or feature requests to C<bug-queue-dbi at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Queue-DBI>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

	perldoc Queue::DBI::Element


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
