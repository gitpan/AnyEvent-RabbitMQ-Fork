package AnyEvent::RabbitMQ::Fork::Worker;
$AnyEvent::RabbitMQ::Fork::Worker::VERSION = '0.1';
=head1 NAME

AnyEvent::RabbitMQ::Fork::Worker - Fork side magic

=head1 DESCRIPTION

No user serviceable parts inside. Venture at your own risk.

=cut

use Moo;
use Types::Standard qw(InstanceOf Bool);
use Guard;
use Scalar::Util qw(weaken blessed);

use namespace::clean;

use AnyEvent::RabbitMQ;

has verbose => (is => 'rw', isa => Bool, default => 0);

has connection => (
    is      => 'lazy',
    isa     => InstanceOf['AnyEvent::RabbitMQ'],
    clearer => 1,
    handles => ['channels'],
);

sub _build_connection {
    my $self = shift;

    my $conn = AnyEvent::RabbitMQ->new(verbose => $self->verbose);

    _cb_hooks($conn);

    return $conn;
}

### RPC Interface ###

my $instance;

sub init {
    my $class = shift;
    $instance = $class->new(@_);
    return;
}

sub run {
    my ($done, $method, $ch_id, @args, %args) = @_;

    weaken(my $self = $instance);

    unless (@args % 2) {
        %args = @args;
        @args = ();
        foreach my $event (grep { /^on_/ } keys %args) {
            # callback signature provided by parent process
            my $sig = delete $args{$event};

            my $guard = guard {
                # inform parent process that this callback is no longer needed
                AnyEvent::Fork::RPC::event(cbd => @$sig);
            };

            # our callback to be used by AE::RMQ
            $args{$event} = sub {
                $guard if 0;    # keepalive

                $self->clear_connection
                  if $sig->[-1] eq 'AnyEvent::RabbitMQ'
                  and ($method eq 'close'
                    or ($method eq 'connect' and $event eq 'on_close'));

                if ((my $isa = blessed $_[0] || q{}) =~ /^AnyEvent::RabbitMQ/) {
                    my $obj = shift;
                    if ($method eq 'open_channel' and $event eq 'on_success') {
                        my $id = $obj->id;    # $ch_id == 0 in this scope
                        $obj->{"_$self\_guard"} ||= guard {
                            AnyEvent::Fork::RPC::event(chd => $id);
                        };

                        _cb_hooks($obj);
                    }

                    unshift @_,
                      \[
                        $isa,
                        ($isa eq 'AnyEvent::RabbitMQ::Channel' ? $obj->id : ())
                       ];
                }

                # these values don't pass muster with Storable
                delete local @{ $_[0] }{ 'fh', 'on_error', 'on_drain' }
                  if $method eq 'connect'
                  and $event = 'on_failure'
                  and blessed $_[0];

                AnyEvent::Fork::RPC::event(cb => $sig, @_);
            };
        }
    }

    if (defined $ch_id and my $ch = $self->channels->{ $ch_id }) {
        $ch->$method(@args ? @args : %args);

        $done->();
    } elsif (defined $ch_id and $ch_id == 0) {
        if ($method eq 'DEMOLISH') {
            $self->clear_connection;
        } else {
            $self->connection->$method(@args ? @args : %args);
        }

        $done->();
    } else {
        $ch_id ||= '<undef>';
        $done->("Unknown channel: '$ch_id'");
    }

    return;
}

my %cb_hooks = (
    channel => {
        _state      => 'is_open',
        _is_active  => 'is_active',
        _is_confirm => 'is_confirm',
    },
    connection => {
        _state             => 'is_open',
        _login_user        => 'login_user',
        _server_properties => 'server_properties',
    }
);
sub _cb_hooks {
    weaken(my $obj = shift);

    my ($type, $hooks)
      = $obj->isa('AnyEvent::RabbitMQ')
      ? ('connection', $cb_hooks{connection})
      : ($obj->id, $cb_hooks{channel});

    while (my ($prop, $method) = each %$hooks) {
        ## no critic (Miscellanea::ProhibitTies)
        tie $obj->{$prop}, 'AnyEvent::RabbitMQ::Fork::Worker::TieScalar',
          $obj->{$prop}, sub {
            AnyEvent::Fork::RPC::event(
                i => { $type => { $method => $obj->$method } });
          };
    }

    return;
}

=head1 AUTHOR

William Cox <mydimension@gmail.com>

=head1 COPYRIGHT

Copyright (c) 2014, the above named author(s).

=head1 LICENSE

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

package    # hide from PAUSE
  AnyEvent::RabbitMQ::Fork::Worker::TieScalar;

use strict;
use warnings;

sub TIESCALAR { return bless [$_[1], $_[2]] => $_[0] }
sub FETCH { return $_[0][0] }
sub STORE { $_[0][1]->(); return $_[0][0] = $_[1] }
sub DESTROY { return @{ $_[0] } = () }

1;
