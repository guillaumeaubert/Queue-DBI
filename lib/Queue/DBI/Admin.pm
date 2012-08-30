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

Version 1.8.2

=cut

our $VERSION = '1.8.2';


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

This program is free software; you can redistribute it and/or modify it
under the terms of the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
