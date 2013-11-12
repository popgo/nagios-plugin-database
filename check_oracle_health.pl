#! /usr/bin/perl -w

my %ERRORS=( OK => 0, WARNING => 1, CRITICAL => 2, UNKNOWN => 3 );
my %ERRORCODES=( 0 => 'OK', 1 => 'WARNING', 2 => 'CRITICAL', 3 => 'UNKNOWN' );
package DBD::Oracle::Server::Instance::SGA::DataBuffer;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    sum_physical_reads => undef,
    sum_physical_reads_direct => undef,
    sum_physical_reads_direct_lob => undef,
    sum_session_logical_reads => undef,
    hitratio => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}


sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga::databuffer::hitratio/) {
    ($self->{sum_physical_reads}, $self->{sum_physical_reads_direct},
        $self->{sum_physical_reads_direct_lob},
        $self->{sum_session_logical_reads}) =
        $self->{handle}->fetchrow_array(q{
          SELECT SUM(DECODE(name, 'physical reads', value, 0)),
              SUM(DECODE(name, 'physical reads direct', value, 0)),
              SUM(DECODE(name, 'physical reads direct (lob)', value, 0)),
              SUM(DECODE(name, 'session logical reads', value, 0))
          FROM sys.v_$sysstat
        });
    if (! defined $self->{sum_physical_reads}) {
      $self->add_nagios_critical("unable to get sga buffer cache");
    } else {
      $self->valdiff(\%params, qw(sum_physical_reads sum_physical_reads_direct
          sum_physical_reads_direct_lob sum_session_logical_reads));
      $self->{hitratio} = $self->{delta_sum_session_logical_reads} ?
          100 - 100 * ((
              $self->{delta_sum_physical_reads} -
              $self->{delta_sum_physical_reads_direct_lob} -
              $self->{delta_sum_physical_reads_direct}) /
              $self->{delta_sum_session_logical_reads}) : 0;
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::sga::databuffer::hitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{hitratio}, "98:", "95:"),
          sprintf "SGA data buffer hit ratio %.2f%%", $self->{hitratio});
      $self->add_perfdata(sprintf "sga_data_buffer_hit_ratio=%.2f%%;%s;%s",
          $self->{hitratio}, 
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA::SharedPool::DictionaryCache;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA::SharedPool);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    sum_gethits => undef,
    sum_gets => undef,
    hitratio => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~
      /server::instance::sga::sharedpool::dictionarycache::hitratio/) {
    ($self->{sum_gets}, $self->{sum_gethits}) =
        $self->{handle}->fetchrow_array(q{
          SELECT SUM(gets), SUM(gets-getmisses) FROM v$rowcache
        });     
    if (! defined $self->{sum_gets}) {
      $self->add_nagios_critical("unable to get sga dc");
    } else {
      $self->valdiff(\%params, qw(sum_gets sum_gethits));
      $self->{hitratio} = $self->{delta_sum_gets} ?
          (100 * $self->{delta_sum_gethits} / $self->{delta_sum_gets}) : 0;
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~
        /server::instance::sga::sharedpool::dictionarycache::hitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{hitratio}, "95:", "90:"),
          sprintf "SGA dictionary cache hit ratio %.2f%%", $self->{hitratio});
      $self->add_perfdata(sprintf "sga_dictionary_cache_hit_ratio=%.2f%%;%s;%s",
          $self->{hitratio}, $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA::SharedPool::LibraryCache;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA::SharedPool);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    sum_gets => undef,
    sum_gethits => undef,
    sum_pins => undef,
    sum_pinhits => undef,
    get_hitratio => undef,
    pin_hitratio => undef,
    reloads => undef,
    invalidations => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ 
      /server::instance::sga::sharedpool::librarycache::(reloads|.*hitratio)/) {
    # http://download.oracle.com/docs/cd/B10500_01/server.920/a96533/sqlviews.htm
    # Look for the following when querying this view:
    # 
    # High RELOADS or INVALIDATIONS
    # Low GETHITRATIO or GETPINRATIO
    #
    # High number of RELOADS could be due to the following:
    #
    # Objects being invalidated (large number of INVALIDATIONS)
    # Objects getting swapped out of memory
    #
    # Low GETHITRATIO could indicate that objects are getting swapped out of memory.
    #
    # Low PINHITRATIO could indicate the following:
    #
    # Session not executing the same cursor multiple times (even though it might be shared across different sessions)
    # Session not finding the cursor shared
    #
    # The next step is to query V$DB_OBJECT_CACHE/V$SQLAREA to see if problems are limited to certain objects or spread across different objects. If invalidations are high, then it might be worth investigating which of the (invalidated object's) underlying objects are being changed.
    #
    ($self->{sum_gethits}, $self->{sum_gets}, $self->{sum_pinhits},
        $self->{sum_pins}, $self->{reloads}, $self->{invalidations}) =
        $self->{handle}->fetchrow_array(q{
            SELECT SUM(gethits), SUM(gets), SUM(pinhits), SUM(pins),
              SUM(reloads), SUM(invalidations)
            FROM v$librarycache
        });
    if (! defined $self->{sum_gets} || ! defined $self->{sum_pinhits}) {
      $self->add_nagios_critical("unable to get sga lc");
    } else {
      $self->valdiff(\%params, qw(sum_gets sum_gethits sum_pins sum_pinhits reloads invalidations));
      $self->{get_hitratio} = $self->{delta_sum_gets} ? 
          (100 * $self->{delta_sum_gethits} / $self->{delta_sum_gets}) : 0;
      $self->{pin_hitratio} = $self->{delta_sum_pins} ? 
          (100 * $self->{delta_sum_pinhits} / $self->{delta_sum_pins}) : 0;
      $self->{reload_rate} = $self->{delta_reloads} / $self->{delta_timestamp};
      $self->{invalidation_rate} = $self->{delta_invalidations} / $self->{delta_timestamp};
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ 
        /server::instance::sga::sharedpool::librarycache::(get)*hitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{get_hitratio}, "98:", "95:"),
          sprintf "SGA library cache (get) hit ratio %.2f%%", $self->{get_hitratio});
      $self->add_perfdata(sprintf "sga_library_cache_hit_ratio=%.2f%%;%s;%s",
          $self->{get_hitratio}, $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::instance::sga::sharedpool::librarycache::pinhitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{pin_hitratio}, "98:", "95:"),
          sprintf "SGA library cache (pin) hit ratio %.2f%%", $self->{get_hitratio});
      $self->add_perfdata(sprintf "sga_library_cache_hit_ratio=%.2f%%;%s;%s",
          $self->{get_hitratio}, $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::instance::sga::sharedpool::librarycache::reloads/) {
      $self->add_nagios(
          $self->check_thresholds($self->{reload_rate}, "10", "100"),
          sprintf "SGA library cache reloads %.2f/sec", $self->{reload_rate});
      $self->add_perfdata(sprintf "sga_library_cache_reloads_per_sec=%.2f;%s;%s",
          $self->{reload_rate}, $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "sga_library_cache_invalidations_per_sec=%.2f",
          $self->{invalidation_rate});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA::SharedPool;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    free => undef,
    reloads => undef,
    pins => undef,
    handle => $params{handle},
    library_cache => undef,
    dictionary_cache => undef,
    parse_soft => undef,
    parse_hard => undef,
    parse_failures => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga::sharedpool::librarycache/) {
    $self->{library_cache} = 
        DBD::Oracle::Server::Instance::SGA::SharedPool::LibraryCache->new(
        %params);
  } elsif ($params{mode} =~ /server::instance::sga::sharedpool::dictionarycache/) {
    $self->{dictionary_cache} = 
        DBD::Oracle::Server::Instance::SGA::SharedPool::DictionaryCache->new(
        %params);
  } elsif ($params{mode} eq "server::instance::sga::sharedpool::free") {
    $self->init_shared_pool_free(%params);
  } elsif ($params{mode} eq "server::instance::sga::sharedpool::reloads") {
    $self->init_shared_pool_reloads(%params);
  } elsif ($params{mode} eq "server::instance::sga::sharedpool::softparse") {
    $self->init_shared_pool_parser(%params);
  }
}

sub init_shared_pool_reloads {
  my $self = shift;
  my %params = @_;
  ($self->{reloads}, $self->{pins}) = $self->{handle}->fetchrow_array(q{
      SELECT SUM(reloads), SUM(pins)
      FROM v$librarycache
      WHERE namespace IN ('SQL AREA','TABLE/PROCEDURE','BODY','TRIGGER')
  });
  if (! defined $self->{reloads}) {
    $self->add_nagios_critical("unable to get sga reloads");
  } else {
    $self->valdiff(\%params, qw(reloads pins));
    $self->{reload_ratio} = $self->{delta_pins} ?
        100 * $self->{delta_reloads} / $self->{delta_pins} : 100;
  }
}

sub init_shared_pool_free {
  my $self = shift;
  my %params = @_;
  if (DBD::Oracle::Server::return_first_server()->version_is_minimum("9.x")) {
    $self->{free_percent} = $self->{handle}->fetchrow_array(q{
        SELECT ROUND(a.bytes / b.sm * 100,2) FROM
          (SELECT bytes FROM v$sgastat 
              WHERE name='free memory' AND pool='shared pool') a,
          (SELECT SUM(bytes) sm FROM v$sgastat 
              WHERE pool = 'shared pool' AND bytes <= 
                  (SELECT bytes FROM v$sgastat 
                      WHERE name='free memory' AND pool='shared pool')) b
    });
  } else {
    # i don't know if the above code works for 8.x, so i leave the old one here
    $self->{free_percent} = $self->{handle}->fetchrow_array(q{
        SELECT ROUND((SUM(DECODE(name, 'free memory', bytes, 0)) /
            SUM(bytes)) * 100,2) FROM v$sgastat where pool = 'shared pool'
    });
  }
  if (! defined $self->{free_percent}) {
    $self->add_nagios_critical("unable to get sga free");
    return undef;
  }
}

sub init_shared_pool_parser {
  my $self = shift;
  my %params = @_;
  ($self->{parse_total}, $self->{parse_hard}, $self->{parse_failures}) = 
      $self->{handle}->fetchrow_array(q{
    SELECT 
      (SELECT value FROM v$sysstat WHERE name = 'parse count (total)'),
      (SELECT value FROM v$sysstat WHERE name = 'parse count (hard)'),
      (SELECT value FROM v$sysstat WHERE name = 'parse count (failures)')
     FROM DUAL
  });
  if (! defined $self->{parse_total}) {
    $self->add_nagios_critical("unable to get parser");
  } else {
    $self->valdiff(\%params, qw(parse_total parse_hard parse_failures));
    $self->{parse_soft_ratio} = $self->{delta_parse_total} ?
      100 * ($self->{delta_parse_total} - $self->{delta_parse_hard}) /
          $self->{delta_parse_total} : 100;
  }
}


sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::sga::sharedpool::librarycache/) {
      $self->{library_cache}->nagios(%params);
      $self->merge_nagios($self->{library_cache});
    } elsif ($params{mode} =~ /server::instance::sga::sharedpool::dictionarycache/) {
      $self->{dictionary_cache}->nagios(%params);
      $self->merge_nagios($self->{dictionary_cache});
    } elsif ($params{mode} eq "server::instance::sga::sharedpool::free") {
      $self->add_nagios(
          $self->check_thresholds($self->{free_percent}, "10:", "5:"),
          sprintf "SGA shared pool free %.2f%%", $self->{free_percent});
      $self->add_perfdata(sprintf "sga_shared_pool_free=%.2f%%;%s;%s",
          $self->{free_percent}, $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} eq "server::instance::sga::sharedpool::reloads") {
      $self->add_nagios(
          $self->check_thresholds($self->{reload_ratio}, "1", "10"),
          sprintf "SGA shared pool reload ratio %.2f%%", $self->{reload_ratio});
      $self->add_perfdata(sprintf "sga_shared_pool_reload_ratio=%.2f%%;%s;%s",
          $self->{reload_ratio}, $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} eq "server::instance::sga::sharedpool::softparse") {
      $self->add_nagios(
          $self->check_thresholds( $self->{parse_soft_ratio}, "98:", "90:"),
          sprintf "Soft parse ratio %.2f%%", $self->{parse_soft_ratio});
      $self->add_perfdata(sprintf "soft_parse_ratio=%.2f%%;%s;%s",
          $self->{parse_soft_ratio},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}

package DBD::Oracle::Server::Instance::SGA::RollbackSegments;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA);


# only create one object with new which stands for all rollback segments

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    gets => undef,
    waits => undef,
    wraps => undef,
    extends => undef,
    undo_header_waits => undef,
    undo_block_waits => undef,
    rollback_segment_hit_ratio => undef,
    rollback_segment_header_contention => undef,
    rollback_segment_block_contention => undef,
    rollback_segment_extents => undef,
    rollback_segment_wraps => undef, 
    rollback_segment_wraps_persec => undef, 
    rollback_segment_extends => undef, 
    rollback_segment_extends_persec => undef, 
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga::rollbacksegments::wraps/) {
    $self->{wraps} = $self->{handle}->fetchrow_array(q{
        SELECT SUM(wraps) FROM v$rollstat
    });
    if (! defined $self->{wraps}) {
      $self->add_nagios_critical("unable to get rollback segments stats");
    } else {
      $self->valdiff(\%params, qw(wraps));
      $self->{rollback_segment_wraps} = $self->{delta_wraps};
      $self->{rollback_segment_wraps_persec} = $self->{delta_wraps} / 
         $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ 
      /server::instance::sga::rollbacksegments::extends/) {
    $self->{extends} = $self->{handle}->fetchrow_array(q{
        SELECT SUM(extends) FROM v$rollstat
    });
    if (! defined $self->{extends}) {
      $self->add_nagios_critical("unable to get rollback segments stats");
    } else {
      $self->valdiff(\%params, qw(extends));
      $self->{rollback_segment_extends} = $self->{delta_extends};
      $self->{rollback_segment_extends_persec} = $self->{delta_extends} /
         $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~
      /server::instance::sga::rollbacksegments::headercontention/) {
    ($self->{undo_header_waits}, $self->{waits})  = $self->{handle}->fetchrow_array(q{
        SELECT ( 
          SELECT SUM(count)
          FROM v$waitstat
          WHERE class = 'undo header' OR class = 'system undo header'
        ) undo, (
          SELECT SUM(count)
          FROM v$waitstat
        ) complete
        FROM DUAL
    });
    if (! defined $self->{undo_header_waits}) {
      $self->add_nagios_critical("unable to get rollback segments wait stats");
    } else {
      $self->valdiff(\%params, qw(undo_header_waits waits));
      $self->{rollback_segment_header_contention} =
          $self->{delta_waits} ? 100 * $self->{delta_undo_header_waits} / $self->{delta_waits} : 0;
    }
  } elsif ($params{mode} =~
      /server::instance::sga::rollbacksegments::blockcontention/) {
    ($self->{undo_block_waits}, $self->{waits})  = $self->{handle}->fetchrow_array(q{
        SELECT ( 
          SELECT SUM(count)
          FROM v$waitstat
          WHERE class = 'undo block' OR class = 'system undo block'
        ) undo, (
          SELECT SUM(count)
          FROM v$waitstat
        ) complete
        FROM DUAL
    });
    if (! defined $self->{undo_block_waits}) { 
      $self->add_nagios_critical("unable to get rollback segments wait stats");
    } else {
      $self->valdiff(\%params, qw(undo_block_waits waits));
      $self->{rollback_segment_block_contention} =
          $self->{delta_waits} ? 100 * $self->{delta_undo_block_waits} / $self->{delta_waits} : 0;
    }
  } elsif ($params{mode} =~
      /server::instance::sga::rollbacksegments::hitratio/) {
    ($self->{waits}, $self->{gets}) = $self->{handle}->fetchrow_array(q{
        SELECT SUM(waits), SUM(gets) FROM v$rollstat
    });
    if (! defined $self->{gets}) {
      $self->add_nagios_critical("unable to get rollback segments wait stats");
    } else {
      $self->valdiff(\%params, qw(waits gets));
      $self->{rollback_segment_hit_ratio} = $self->{delta_gets} ?
          100 - 100 * $self->{delta_waits} / $self->{delta_gets} : 100;
    }
  } elsif ($params{mode} =~
      /server::instance::sga::rollbacksegments::avgactivesize/) {
    if ($params{selectname}) {
      $self->{rollback_segment_optimization_size} = $self->{handle}->fetchrow_array(q{
          SELECT AVG(s.optsize / 1048576) optmization_size
          FROM v$rollstat s, v$rollname n
          WHERE s.usn = n.usn AND n.name != 'SYSTEM' AND n.name = ?
      }, $params{selectname}) || 0;
      $self->{rollback_segment_average_active} = $self->{handle}->fetchrow_array(q{
          SELECT AVG(s.aveactive / 1048576) average_active
          FROM v$rollstat s, v$rollname n
          WHERE s.usn = n.usn AND n.name != 'SYSTEM' AND n.name = ? 
      }, $params{selectname}) || 0;
    } else {
      $self->{rollback_segment_optimization_size} = $self->{handle}->fetchrow_array(q{
          SELECT AVG(s.optsize / 1048576) optmization_size
          FROM v$rollstat s, v$rollname n
          WHERE s.usn = n.usn AND n.name != 'SYSTEM' 
      }) || 0;
      $self->{rollback_segment_average_active} = $self->{handle}->fetchrow_array(q{
          SELECT AVG(s.aveactive / 1048576) average_active
          FROM v$rollstat s, v$rollname n
          WHERE s.usn = n.usn AND n.name != 'SYSTEM'
      }) || 0;
    }
  } else {
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::sga::rollbacksegments::wraps/) {
      if ($params{absolute}) {
        $self->add_nagios(
            $self->check_thresholds(
                $self->{rollback_segment_wraps}, "1", "100"),
            sprintf "Rollback segment wraps %d times",
                $self->{rollback_segment_wraps});
      } else {
        $self->add_nagios(
            $self->check_thresholds(
                $self->{rollback_segment_wraps_persec}, "1", "100"),
            sprintf "Rollback segment wraps %.2f/sec",
                $self->{rollback_segment_wraps_persec});
      }
      $self->add_perfdata(
          sprintf "rollback_segment_wraps=%d;%s;%s",
              $self->{rollback_segment_wraps},
              $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(
          sprintf "rollback_segment_wraps_rate=%.2f;%s;%s",
              $self->{rollback_segment_wraps_persec},
              $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::instance::sga::rollbacksegments::extends/) {
      if ($params{absolute}) {
        $self->add_nagios(
            $self->check_thresholds(
                $self->{rollback_segment_extends}, "1", "100"),
            sprintf "Rollback segment extends %d times",
                $self->{rollback_segment_extends});
      } else {
        $self->add_nagios(
            $self->check_thresholds(
                $self->{rollback_segment_extends_persec}, "1", "100"),
            sprintf "Rollback segment extends %.2f/sec",
                $self->{rollback_segment_extends_persec});
      }
      $self->add_perfdata(
          sprintf "rollback_segment_extends=%d;%s;%s",
              $self->{rollback_segment_extends},
              $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(
          sprintf "rollback_segment_extends_rate=%.2f;%s;%s",
              $self->{rollback_segment_extends_persec},
              $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::instance::sga::rollbacksegments::headercontention/) {
      $self->add_nagios(
          $self->check_thresholds(
              $self->{rollback_segment_header_contention}, "1", "2"),
          sprintf "Rollback segment header contention is %.2f%%",
              $self->{rollback_segment_header_contention});
      $self->add_perfdata(
          sprintf "rollback_segment_header_contention=%.2f%%;%s;%s",
              $self->{rollback_segment_header_contention},
              $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::instance::sga::rollbacksegments::blockcontention/) {
      $self->add_nagios(
          $self->check_thresholds(
              $self->{rollback_segment_block_contention}, "1", "2"),
          sprintf "Rollback segment block contention is %.2f%%",
              $self->{rollback_segment_block_contention});
      $self->add_perfdata(
          sprintf "rollback_segment_block_contention=%.2f%%;%s;%s",
              $self->{rollback_segment_block_contention},
              $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::instance::sga::rollbacksegments::hitratio/) {
      $self->add_nagios(
          $self->check_thresholds(
              $self->{rollback_segment_hit_ratio}, "99:", "98:"),
          sprintf "Rollback segment hit ratio is %.2f%%",
              $self->{rollback_segment_hit_ratio});
      $self->add_perfdata(
		  sprintf "rollback_segment_hit_ratio=%.2f%%;%s;%s",
              $self->{rollback_segment_hit_ratio},
              $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::instance::sga::rollbacksegments::avgactivesize/) {
      $self->add_nagios_ok(sprintf "Rollback segment average size %.2f MB",
          $self->{rollback_segment_average_active});
      $self->add_perfdata(
          sprintf "rollback_segment_avgsize=%.2f rollback_segment_optsize=%.2f",
              $self->{rollback_segment_average_active},
              $self->{rollback_segment_optimization_size});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA::RedoLogBuffer;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    last_switch_interval => undef,
    redo_buffer_allocation_retries => undef,
    redo_entries => undef,
    retry_ratio => undef,
    redo_size => undef,
    redo_size_per_sec => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}


sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga::redologbuffer::switchinterval/) {
    if ($self->instance_rac()) {
      eval {
        # alles was jemals geswitcht hat, letzter switch, zweitletzter switch
        # jetzt - letzter switch = mindestlaenge des naechsten intervals
        # wenn das lang genug ist, dann war das letzte, kurze intervall
        # wohl nur ein ausreisser oder manueller switch
        # derzeit laufendes intervall, letztes intervall, vorletztes intervall
        ($self->{next_switch_interval}, $self->{last_switch_interval}, $self->{nextto_last_switch_interval}) =
            $self->{handle}->fetchrow_array(q {
          WITH temptab AS
          (
            SELECT sequence#, first_time FROM sys.v_$log WHERE status = 'CURRENT'
                AND thread# = ?
            UNION ALL
            SELECT sequence#, first_time FROM sys.v_$log_history 
                WHERE thread# = ?
                ORDER BY first_time DESC
          )
          SELECT 
              (sysdate - a.first_time) * 1440 * 60 thisinterval,
              (a.first_time - b.first_time) * 1440 * 60 lastinterval,
              (b.first_time - c.first_time) * 1440 * 60 nexttolastinterval
          FROM
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 1
                ) WHERE irow = 1
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) a,
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 2
                ) WHERE irow = 2
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) b,
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 3
                ) WHERE irow = 3
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) c
        }, $self->instance_thread(), $self->instance_thread());
      };
    } else {
      eval {
        # alles was jemals geswitcht hat, letzter switch, zweitletzter switch
        # jetzt - letzter switch = mindestlaenge des naechsten intervals
        # wenn das lang genug ist, dann war das letzte, kurze intervall
        # wohl nur ein ausreisser oder manueller switch
        # derzeit laufendes intervall, letztes intervall, vorletztes intervall
        ($self->{next_switch_interval}, $self->{last_switch_interval}, $self->{nextto_last_switch_interval}) =
            $self->{handle}->fetchrow_array(q {
          WITH temptab AS
          (
            SELECT sequence#, first_time FROM sys.v_$log WHERE status = 'CURRENT'
            UNION ALL
            SELECT sequence#, first_time FROM sys.v_$log_history ORDER BY first_time DESC
          )
          SELECT 
              (sysdate - a.first_time) * 1440 * 60 thisinterval,
              (a.first_time - b.first_time) * 1440 * 60 lastinterval,
              (b.first_time - c.first_time) * 1440 * 60 nexttolastinterval
          FROM
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 1
                ) WHERE irow = 1
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) a,
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 2
                ) WHERE irow = 2
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) b,
          (
            SELECT NVL(
              (
                SELECT first_time FROM (
                  SELECT first_time, rownum AS irow FROM temptab WHERE ROWNUM <= 3
                ) WHERE irow = 3
              ) , to_date('20090624','YYYYMMDD')) as first_time FROM dual
          ) c
        });
      };
    }
    if (! defined $self->{last_switch_interval}) {
      $self->add_nagios_critical(
          sprintf "unable to get last switch interval");
    }
  } elsif ($params{mode} =~ /server::instance::sga::redologbuffer::retryratio/) {
    ($self->{redo_buffer_allocation_retries}, $self->{redo_entries}) = 
        $self->{handle}->fetchrow_array(q{
            SELECT a.value, b.value
            FROM v$sysstat a, v$sysstat b  
            WHERE a.name = 'redo buffer allocation retries'  
            AND b.name = 'redo entries'
    });
    if (! defined $self->{redo_buffer_allocation_retries}) {
      $self->add_nagios_critical("unable to get retry ratio");
    } else {
      $self->valdiff(\%params, qw(redo_buffer_allocation_retries redo_entries));
      $self->{retry_ratio} = $self->{delta_redo_entries} ? 
          100 * $self->{delta_redo_buffer_allocation_retries} / $self->{delta_redo_entries} : 0;
    }
  } elsif ($params{mode} =~ /server::instance::sga::redologbuffer::iotraffic/) {
    $self->{redo_size} = $self->{handle}->fetchrow_array(q{
        SELECT value FROM v$sysstat WHERE name = 'redo size'
    });
    if (! defined $self->{redo_size}) {
      $self->add_nagios_critical("unable to get redo size");
    } else {
      $self->valdiff(\%params, qw(redo_size));
      $self->{redo_size_per_sec} =
          $self->{delta_redo_size} / $self->{delta_timestamp};
      # Megabytes / sec
      $self->{redo_size_per_sec} = $self->{redo_size_per_sec} / 1048576;
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~
        /server::instance::sga::redologbuffer::switchinterval/) {
      my $nextlevel = $self->check_thresholds($self->{next_switch_interval}, "600:", "60:");
      my $nexttolastlevel = $self->check_thresholds($self->{nextto_last_switch_interval}, "600:", "60:");
      my $lastlevel = $self->check_thresholds($self->{last_switch_interval}, "600:", "60:");
      if ($lastlevel) {
        # nachschauen, ob sich die situation schon entspannt hat
        if ($nextlevel == 2) {
          # das riecht nach aerger. kann zwar auch daran liegen, weil der check unmittelbar nach dem kurzen switch
          # ausgefuehrt wird, aber dann bleibts beim soft-hard und beim retry schauts schon besser aus.
          if ($self->{next_switch_interval} < 0) {
            # jetzt geht gar nichts mehr
            $self->add_nagios(
                2,
                "Found a redo log with a timestamp in the future!!");
            $self->{next_switch_interval} = 0;
          } else {
              $self->add_nagios(
                  # 10: minutes, 1: minute = 600:, 60:
                  $nextlevel,
                  sprintf "Last redo log file switch interval was %d minutes%s. Next interval presumably >%d minutes",
                      $self->{last_switch_interval} / 60,
                      $self->instance_rac() ? sprintf " (thread %d)", $self->instance_thread() : "",
                      $self->{next_switch_interval} / 60);
          }
        } elsif ($nextlevel == 1) {
          # das kommt daher, weil retry_interval < warningthreshold
          if ($nexttolastlevel) {
            # aber vorher war auch schon was faul. da braut sich vieleicht was zusammen.
            # die warnung ist sicher berechtigt.
            $self->add_nagios(
                $nextlevel,
                sprintf "Last redo log file switch interval was %d minutes%s. Next interval presumably >%d minutes. Second incident in a row.",
                    $self->{last_switch_interval} / 60,
                    $self->instance_rac() ? sprintf " (thread %d)", $self->instance_thread() : "",
                    $self->{next_switch_interval} / 60);
          } else {
            # hier bin ich grosszuegig. vorletztes intervall war ok, letztes intervall war nicht ok.
            # ich rechne mir also chancen aus, dass $nextlevel nur auf warning ist, weil der retry zu schnell
            # nach dem letzten switch stattfindet. sollte sich entspannen und wenns wirklich ein problem gibt
            # dann kommt sowieso wieder ein switch. also erstmal ok.
            $self->add_nagios(
                0,
                sprintf "Last redo log file switch interval was %d minutes%s. Next interval presumably >%d minutes. Probably a single incident.",
                    $self->{last_switch_interval} / 60,
                    $self->instance_rac() ? sprintf " (thread %d)", $self->instance_thread() : "",
                    $self->{next_switch_interval} / 60);
          }
        } else {
          # war wohl ein einzelfall. also gehen wir davon aus, dass das warninglevel nur wegen des retrys
          # unterschritten wurde und der naechste switch wieder lange genug sein wird
          $self->add_nagios(
              $nextlevel, # sollte 0 sein
              sprintf "Last redo log file switch interval was %d minutes%s. Next interval presumably >%d minutes",
                  $self->{last_switch_interval} / 60,
                  $self->instance_rac() ? sprintf " (thread %d)", $self->instance_thread() : "",
                  $self->{next_switch_interval} / 60);
        }
      } else {
        $self->add_nagios(
            $lastlevel,
            sprintf "Last redo log file switch interval was %d minutes%s. Next interval presumably >%d minutes",
                $self->{last_switch_interval} / 60,
                $self->instance_rac() ? sprintf " (thread %d)", $self->instance_thread() : "",
                $self->{next_switch_interval} / 60);
      }
      $self->add_perfdata(sprintf "redo_log_file_switch_interval=%ds;%s;%s",
          $self->{last_switch_interval},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::instance::sga::redologbuffer::retryratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{retry_ratio}, "1", "10"),
          sprintf "Redo log retry ratio is %.6f%%",$self->{retry_ratio});
      $self->add_perfdata(sprintf "redo_log_retry_ratio=%.6f%%;%s;%s",
          $self->{retry_ratio},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::instance::sga::redologbuffer::iotraffic/) {
      $self->add_nagios(
          $self->check_thresholds($self->{redo_size_per_sec}, "100", "200"),
          sprintf "Redo log io is %.6f MB/sec", $self->{redo_size_per_sec});
      $self->add_perfdata(sprintf "redo_log_io_per_sec=%.6f;%s;%s",
          $self->{redo_size_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA::Latch;

our @ISA = qw(DBD::Oracle::Server::Instance::SGA);


{
  my @latches = ();
  my $initerrors = undef;

  sub add_latch {
    push(@latches, shift);
  }

  sub return_latches {
    my %params = @_;
    if ($params{mode} =~ /server::instance::sga::latch::contention/) {
      return reverse
          sort { $a->{contention} <=> $b->{contention} } @latches;
    } else {
      return reverse
          sort { $a->{name} cmp $b->{name} } @latches;
    }
  }

  sub init_latches {
    my %params = @_;
    my $num_latches = 0;
    if (($params{mode} =~ /server::instance::sga::latch::contention/) ||
        ($params{mode} =~ /server::instance::sga::latch::waiting/) ||
        ($params{mode} =~ /server::instance::sga::latch::hitratio/) ||
        ($params{mode} =~ /server::instance::sga::latch::listlatches/)) {
      my $sumsleeps = $params{handle}->fetchrow_array(q{
          SELECT SUM(sleeps) FROM v$latch
      });
      my @latchresult = $params{handle}->fetchall_array(q{
        SELECT latch#, name, gets, sleeps, misses, wait_time
        FROM v$latch
      });
      foreach (@latchresult) {
        my ($number, $name, $gets, $sleeps, $misses, $wait_time) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if ($params{selectname} && (
              ($params{selectname} !~ /^\d+$/ && (lc $params{selectname} ne lc $name)) || 
              ($params{selectname} =~ /^\d+$/ && ($params{selectname} != $number))));
        }
        my %thisparams = %params;
        $thisparams{number} = $number;
        $thisparams{name} = $name;
        $thisparams{gets} = $gets;
        $thisparams{misses} = $misses;
        $thisparams{sleeps} = $sleeps;
        $thisparams{wait_time} = $wait_time;
        $thisparams{sumsleeps} = $sumsleeps;
        my $latch = DBD::Oracle::Server::Instance::SGA::Latch->new(
            %thisparams);
        add_latch($latch);
        $num_latches++;
      }
      if (! $num_latches) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    number => $params{number},
    name => $params{name},
    gets => $params{gets},
    misses => $params{misses},
    sleeps => $params{sleeps},
    wait_time => $params{wait_time},
    sumsleeps => $params{sumsleeps},
    hitratio => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}


sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ 
      /server::instance::sga::latch::hitratio/) {
    if (! defined $self->{gets}) {
      $self->add_nagios_critical(
          sprintf "unable to get sga latches %s", $self->{name});
    } else {
      $params{differenciator} = lc $self->{name}.$self->{number};
      $self->valdiff(\%params, qw(gets misses));
      $self->{hitratio} = $self->{delta_gets} ? 
          100 * ($self->{delta_gets} - $self->{delta_misses}) / $self->{delta_gets} : 100;
    }
  } elsif (($params{mode} =~ /server::instance::sga::latch::contention/) ||
      ($params{mode} =~ /server::instance::sga::latch::waiting/)) {
    if (! defined $self->{gets}) {
      $self->add_nagios_critical(
          sprintf "unable to get sga latches %s", $self->{name});
    } else {
      $params{differenciator} = lc $self->{name}.$self->{number};
      $self->valdiff(\%params, qw(gets sleeps misses wait_time sumsleeps));
      # latch contention
      $self->{contention} = $self->{delta_gets} ?
          100 * $self->{delta_misses} / $self->{delta_gets} : 0;
      # latch percent of sleep during the elapsed time
      $self->{sleep_share} = $self->{delta_wait_time} ?
          ((100 * $self->{wait_time}) / 1000) / $self->{delta_timestamp} : 0;
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ 
        /server::instance::sga::latch::hitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{hitratio}, "98:", "95:"),
          sprintf "SGA latches hit ratio %.2f%%", $self->{hitratio});
      $self->add_perfdata(sprintf "sga_latches_hit_ratio=%.2f%%;%s;%s",
          $self->{hitratio}, $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::instance::sga::latch::contention/) {
      $self->add_nagios(
          $self->check_thresholds($self->{contention}, "1", "2"),
          sprintf "SGA latch %s (#%d) contention %.2f%%", 
	      $self->{name}, $self->{number}, $self->{contention});
      $self->add_perfdata(sprintf "'latch_%d_contention'=%.2f%%;%s;%s",
          $self->{number}, $self->{contention}, $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "'latch_%d_gets'=%u",
          $self->{number}, $self->{delta_gets});
    } elsif ($params{mode} =~ 
        /server::instance::sga::latch::waiting/) {
      $self->add_nagios(
          $self->check_thresholds($self->{sleep_share}, "0.1", "1"),
          sprintf "SGA latch %s (#%d) sleeping %.6f%% of the time", 
	      $self->{name}, $self->{number}, $self->{sleep_share});
      $self->add_perfdata(sprintf "'latch_%d_sleep_share'=%.6f%%;%s;%s;0;100",
          $self->{number}, $self->{sleep_share}, $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::Oracle::Server::Instance::SGA;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    data_buffer => undef,
    shared_pool => undef,
    latches => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga::databuffer/) {
    $self->{data_buffer} = DBD::Oracle::Server::Instance::SGA::DataBuffer->new(
        %params);
  } elsif ($params{mode} =~ /server::instance::sga::sharedpool/) {
    $self->{shared_pool} = DBD::Oracle::Server::Instance::SGA::SharedPool->new(
        %params);
  } elsif ($params{mode} =~ /server::instance::sga::latch/) {
    DBD::Oracle::Server::Instance::SGA::Latch::init_latches(%params);
    if (my @latches =
        DBD::Oracle::Server::Instance::SGA::Latch::return_latches(%params)) {
      $self->{latches} = \@latches;
    } else {
      $self->add_nagios_critical("unable to aquire latch info");
    }
  } elsif ($params{mode} =~ /server::instance::sga::redolog/) {
    $self->{redo_log_buffer} =
        DBD::Oracle::Server::Instance::SGA::RedoLogBuffer->new(%params);
  } elsif ($params{mode} =~ /server::instance::sga::rollbacksegments/) {
    $self->{rollback_segments} =
        DBD::Oracle::Server::Instance::SGA::RollbackSegments->new(%params);
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if ($params{mode} =~ /server::instance::sga::databuffer/) {
    $self->{data_buffer}->nagios(%params);
    $self->merge_nagios($self->{data_buffer});
  } elsif ($params{mode} =~ /server::instance::sga::sharedpool/) {
    $self->{shared_pool}->nagios(%params);
    $self->merge_nagios($self->{shared_pool});
  } elsif ($params{mode} =~ /server::instance::sga::latch::hitratio/) {
    if (! $self->{nagios_level}) {
      my $hitratio = 0;
      foreach (@{$self->{latches}}) {
        $hitratio = $hitratio + $_->{hitratio};
      }
      $hitratio = $hitratio / scalar(@{$self->{latches}});
      $self->add_nagios(
          $self->check_thresholds($hitratio, "98:", "95:"),
          sprintf "SGA latches hit ratio %.2f%%", $hitratio);
      $self->add_perfdata(sprintf "sga_latches_hit_ratio=%.2f%%;%s;%s",
          $hitratio, $self->{warningrange}, $self->{criticalrange});
    }
  } elsif ($params{mode} =~ /server::instance::sga::latch::listlatches/) {
    foreach (sort { $a->{number} <=> $b->{number} } @{$self->{latches}}) {
      printf "%03d %s\n", $_->{number}, $_->{name};
    }
    $self->add_nagios_ok("have fun");
  } elsif ($params{mode} =~ /server::instance::sga::latch/) {
    foreach (@{$self->{latches}}) {
      $_->nagios(%params);
      $self->merge_nagios($_);
    }
  } elsif ($params{mode} =~ /server::instance::sga::redologbuffer/) {
    $self->{redo_log_buffer}->nagios(%params);
    $self->merge_nagios($self->{redo_log_buffer});
  } elsif ($params{mode} =~ /server::instance::sga::rollbacksegments/) {
    $self->{rollback_segments}->nagios(%params);
    $self->merge_nagios($self->{rollback_segments});
  }
}



package DBD::Oracle::Server::Instance::PGA;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    internals => undef,
    pgas => [],
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::pga/) {
    $self->{internals} =
        DBD::Oracle::Server::Instance::PGA::Internals->new(%params);
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if ($params{mode} =~ /server::instance::pga/) {
    $self->{internals}->nagios(%params);
    $self->merge_nagios($self->{internals});
  }
}


package DBD::Oracle::Server::Instance::PGA::Internals;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance::PGA);

my $internals; # singleton, nur ein einziges mal instantiierbar

sub new {
  my $class = shift;
  my %params = @_;
  unless ($internals) {
    $internals = {
      handle => $params{handle},
      in_memory_sorts => undef,
      in_disk_sorts => undef,
      in_memory_sort_ratio => undef,
      warningrange => $params{warningrange},
      criticalrange => $params{criticalrange},
    };
    bless($internals, $class);
    $internals->init(%params);
  }
  return($internals);
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->debug("enter init");
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::pga::inmemorysortratio/) {
    ($self->{in_memory_sorts}, $self->{in_disk_sorts}) =
        $self->{handle}->fetchrow_array(q{
        SELECT mem.value, dsk.value 
        FROM v$sysstat mem, v$sysstat dsk
        WHERE mem.name='sorts (memory)' AND dsk.name='sorts (disk)'
    });
    if (! defined $self->{in_memory_sorts}) {
      $self->add_nagios_critical("unable to get pga ratio");
    } else {
      $self->valdiff(\%params, qw(in_memory_sorts in_disk_sorts));
      $self->{in_memory_sort_ratio} =
          ($self->{delta_in_memory_sorts} + $self->{delta_in_disk_sorts}) == 0 ? 100 :
          100 * $self->{delta_in_memory_sorts} /
          ($self->{delta_in_memory_sorts} + $self->{delta_in_disk_sorts});
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::pga::inmemorysortratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{in_memory_sort_ratio}, "99:", "90:"),
          sprintf "PGA in-memory sort ratio %.2f%%",
          $self->{in_memory_sort_ratio});
      $self->add_perfdata(sprintf "pga_in_memory_sort_ratio=%.2f%%;%s;%s;0;100",
          $self->{in_memory_sort_ratio},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::Oracle::Server::Instance::Event;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance);


{
  my @events = ();
  my $initerrors = undef;

  sub add_event {
    push(@events, shift);
  }

  sub return_events {
    my %params = @_;
    if ($params{mode} =~ /server::instance::event::waits/) {
      return reverse 
          sort { $a->{waits_per_sec} <=> $b->{waits_per_sec} } @events;
    } elsif ($params{mode} =~ /server::instance::event::waiting/) {
      return reverse 
          sort { $a->{percent_waited} <=> $b->{percent_waited} } @events;
    } else {
      return reverse 
          sort { $a->{name} cmp $b->{name} } @events;
    }
  }

  sub init_events {
    my %params = @_;
    my $num_events = 0;
    my %longnames = ();
    if (($params{mode} =~ /server::instance::event::wait/) || #waits, waiting
        ($params{mode} =~ /server::instance::event::listevents/)) {
      my $sql;
      my @idlewaits = ();
      if (DBD::Oracle::Server::return_first_server()->version_is_minimum("10.x")) {
        @idlewaits = map { $_->[0] } $params{handle}->fetchall_array(q{
            SELECT name FROM v$event_name WHERE  wait_class = 'Idle'
        });
      } elsif (DBD::Oracle::Server::return_first_server()->version_is_minimum("9.x")) {
        @idlewaits = (
            'smon timer',
            'pmon timer',
            'rdbms ipc message',
            'Null event',
            'parallel query dequeue',
            'pipe get',
            'client message',
            'SQL*Net message to client',
            'SQL*Net message from client',
            'SQL*Net more data from client',
            'dispatcher timer',
            'virtual circuit status',
            'lock manager wait for remote message',
            'PX Idle Wait',
            'PX Deq: Execution Msg',
            'PX Deq: Table Q Normal',
            'wakeup time manager',
            'slave wait',
            'i/o slave wait',
            'jobq slave wait',
            'null event',
            'gcs remote message',
            'gcs for action',
            'ges remote message',
            'queue messages',
        );
      }
      if ($params{mode} =~ /server::instance::event::listeventsbg/) {
        if (DBD::Oracle::Server::return_first_server()->version_is_minimum("10.x")) {
          $sql = q{
            SELECT e.event_id, e.event, 0, 0, 0, 0 FROM v$session_event e WHERE e.sid IN 
                (SELECT s.sid FROM v$session s WHERE s.type = 'BACKGROUND') GROUP BY e.event, e.event_id
          };
        } else {
          $sql = q{
            SELECT n.event#, e.event, 0, 0, 0, 0 FROM v$session_event e, v$event_name n
            WHERE n.name = e.event AND e.sid IN 
                (SELECT s.sid FROM v$session s WHERE s.type = 'BACKGROUND') GROUP BY e.event, n.event#
          };
        } 
      } else {
        if (DBD::Oracle::Server::return_first_server()->version_is_minimum("10.x")) {
          $sql = q{
            SELECT e.event_id, e.name, 
                NVL(s.total_waits, 0), NVL(s.total_timeouts, 0), NVL(s.time_waited, 0),
                NVL(s.time_waited_micro, 0), NVL(s.average_wait, 0)
            FROM v$event_name e LEFT JOIN sys.v_$system_event s ON e.name = s.event
          };
        } else {
          $sql = q{
            SELECT e.event#, e.name, 
                NVL(s.total_waits, 0), NVL(s.total_timeouts, 0), NVL(s.time_waited, 0),
                NVL(s.time_waited_micro, 0), NVL(s.average_wait, 0)
            FROM v$event_name e LEFT JOIN sys.v_$system_event s ON e.name = s.event
          };
        }
      }
      my @eventresults = $params{handle}->fetchall_array($sql);
      foreach (@eventresults) {
        my ($event_no, $name, $total_waits, $total_timeouts, 
            $time_waited, $time_waited_micro, $average_wait) = @{$_};
	$longnames{$name} = "";
      }
      abbreviate(\%longnames, 2);
      foreach (@eventresults) {
        my ($event_no, $name, $total_waits, $total_timeouts, 
            $time_waited, $time_waited_micro, $average_wait) = @{$_};
        my $shortname = $longnames{$name}->{abbreviation};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if ($params{selectname} && (
              (($params{selectname} !~ /^\d+$/) &&
                  (! grep /^$params{selectname}$/, map { $longnames{$_}->{abbreviation} } 
                      keys %longnames) &&
                  (lc $params{selectname} ne lc $name)) ||
              (($params{selectname} !~ /^\d+$/) &&
                  (grep /^$params{selectname}$/, map { $longnames{$_}->{abbreviation} } 
                      keys %longnames) &&
                  (lc $params{selectname} ne lc $shortname)) ||
              ($params{selectname} =~ /^\d+$/ &&
                  ($params{selectname} != $event_no))));
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{shortname} = $shortname;
        $thisparams{event_id} = $event_no;   # bei > 10.x unbedingt event_id aus db holen
        $thisparams{total_waits} = $total_waits;
        $thisparams{total_timeouts} = $total_timeouts;
        $thisparams{time_waited} = $time_waited;
        $thisparams{time_waited_micro} = $time_waited_micro;
        $thisparams{average_wait} = $average_wait;
        $thisparams{idle} = scalar(grep { lc $name =~ /$_/ } @idlewaits);
        my $event = DBD::Oracle::Server::Instance::Event->new(
            %thisparams);
        add_event($event);
        $num_events++;
      }
      if (! $num_events) {
        $initerrors = 1;
        return undef;
      }
    }
  }

  sub begindiff {
    # liefere indices fuer das erste untersch. wort und innerhalb diesem das erste untersch. zeichen
    my @names = @_;
    my $len = 100;
    my $first_diff_word = 0;
    my $first_diff_pos = 0;
    my $smallest_wordcnt = (sort { $a->{wordcnt} <=> $b->{wordcnt} } @names)[0]->{wordcnt};
    foreach my $wordno (0..$smallest_wordcnt-1) {
      my $wordequal = 1;
      my $refword = @{$names[0]->{words}}[$wordno];
      foreach (@names) {
        if (@{$_->{words}}[$wordno] ne $refword) {
          $wordequal = 0;
        }
      }
      $first_diff_word = $wordno;
      if (! $wordequal) {
        last;
      }
    }
    my $smallest_wordlen = 
        length(${(sort { length(${$a->{words}}[$first_diff_word]) <=> length(${$b->{words}}[$first_diff_word])  } @names)[0]->{words}}[$first_diff_word]);
    foreach my $posno (0..$smallest_wordlen-1) {
      my $posequal = 1;
      my $refpos = substr(@{$names[0]->{words}}[$first_diff_word], $posno, 1);
      foreach (@names) {
        if (substr(@{$_->{words}}[$first_diff_word], $posno, 1) ne $refpos) {
          $posequal = 0;
        }
      }
      $first_diff_pos = $posno;
      if (! $posequal) {
        last;
      }
    }
    return ($first_diff_word, $first_diff_pos);
  }

  sub abbreviate {
    #
    # => zeiger auf hash, dessen keys lange namen sind
    # <= gleicher hash mit ausgefuellten eindeutigen values
    #
    my $names = shift;
    my %done = ();
    my $collisions = {};
    foreach my $long (keys %{$names}) {
      # erstmal das noetige werkzeug schmieden
      # und kurzbezeichnungen aus jeweils zwei zeichen bilden
      $names->{$long} = {};
      $names->{$long}->{words} = [
          map { lc }
          map { my $x = $_; $x =~ s/[()\/\-]//g; $x }
          map { /^\-$/ ? () : $_ } 
          split(/_|\s+/, $long) ];
      $names->{$long}->{wordcnt} = scalar (@{$names->{$long}->{words}});
      $names->{$long}->{shortwords} = [ map { substr $_, 0, 2 } @{$names->{$long}->{words}} ];
      $names->{$long}->{abbreviation} = join("_", @{$names->{$long}->{shortwords}});
      $names->{$long}->{unique} = 1;
    }
    individualize($names, -1, -1);
  }

  sub individualize {
    my $names = shift;
    my $delword = shift;
    my $delpos = shift;
    my %done = ();
    my $collisions = {};
    if ($delword >= 0 && $delpos >= 0) {
      # delpos ist die position mit dem ersten unterschied. kann fuer den kuerzesten string 
      # schon nicht mehr existieren.
      map { 
        if (length(${$names->{$_}->{words}}[$delword]) > 2) {
          
          if (length(${$names->{$_}->{words}}[$delword]) == $delpos) {
            ${$names->{$_}->{shortwords}}[$delword] =
                substr(${$names->{$_}->{words}}[$delword], 0, 2)
          } else {
            ${$names->{$_}->{shortwords}}[$delword] =
                substr(${$names->{$_}->{words}}[$delword], 0, 1).
                substr(${$names->{$_}->{words}}[$delword], $delpos);
          }
        }
      } keys %{$names};
    }
    map { $names->{$_}->{abbreviation} = join("_", @{$names->{$_}->{shortwords}}) } keys %{$names};
    map { $done{$names->{$_}->{abbreviation}}++ } keys %{$names};
    map { $names->{$_}->{unique} = $done{$names->{$_}->{abbreviation}} > 1 ? 0 : 1 } keys %{$names};
    #
    #  hash mit abkuerzung als key und array(langnamen, ...) als value.
    #  diese sind nicht eindeutig und muessen noch geschickter abgekuerzt werden
    #
    foreach my $collision (map { $names->{$_}->{unique} ? () : $_ } keys %{$names}) {
      if (! exists $collisions->{$names->{$collision}->{abbreviation}}) {
        $collisions->{$names->{$collision}->{abbreviation}} = [];
      }
      push(@{$collisions->{$names->{$collision}->{abbreviation}}}, $collision);
    }
    #
    # jeweils gruppen mit gemeinsamer, mehrdeutiger abkuerzung werden nochmals gerechnet
    #
    foreach my $collision (keys %{$collisions}) {
      my $newnames = {};
      # hilfestellung, wo es unterschiede gibt
      my($wordnum, $posnum) = begindiff(map { $names->{$_} } @{$collisions->{$collision}});
      map { $newnames->{$_} = 
          $names->{$_} } grep { $names->{$_}->{abbreviation} eq $collision } keys %{$names};
      individualize($newnames, $wordnum, $posnum);
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    shortname => $params{shortname},
    event_id => $params{event_id}, # > 10.x
    total_waits => $params{total_waits},
    total_timeouts => $params{total_timeouts},
    time_waited => $params{time_waited}, # divide by 100
    time_waited_micro => $params{time_waited_micro}, # divide by 1000000
    average_wait => $params{average_wait},
    idle => $params{idle} || 0,
    waits_per_sec => undef,
    percent_waited => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  #$self->{name} =~ s/^\s+//;
  #$self->{name} =~ s/\s+$//;
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::event::wait/) {
    if (! defined $self->{total_waits}) {
      $self->add_nagios_critical("unable to get event info");
    } else {
      $params{differenciator} = lc $self->{name};
      $self->valdiff(\%params, qw(total_waits total_timeouts time_waited
          time_waited_micro average_wait));
      $self->{waits_per_sec} = 
          $self->{delta_total_waits} / $self->{delta_timestamp};
      $self->{percent_waited} = 
          100 * ($self->{delta_time_waited_micro} / 1000000 ) / $self->{delta_timestamp};
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::event::waits/) {
      $self->add_nagios(
          $self->check_thresholds($self->{waits_per_sec}, "10", "100"),
          sprintf "%s : %.6f waits/sec", $self->{name}, $self->{waits_per_sec});
      $self->add_perfdata(sprintf "'%s_waits_per_sec'=%.6f;%s;%s",
          $self->{name}, 
          $self->{waits_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::event::waiting/) {
      $self->add_nagios(
          $self->check_thresholds($self->{percent_waited}, "0.1", "0.5"),
          sprintf "%s waits %.6f%% of the time", $self->{name}, $self->{percent_waited});
      $self->add_perfdata(sprintf "'%s_percent_waited'=%.6f%%;%s;%s",
          $self->{name}, 
          $self->{percent_waited},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}




package DBD::Oracle::Server::Instance::Enqueue;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance);


{
  my @enqueues = ();
  my $initerrors = undef;

  sub add_enqueue {
    push(@enqueues, shift);
  }

  sub return_enqueues {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @enqueues;
  }

  sub init_enqueues {
    my %params = @_;
    my $num_enqueues = 0;
    if (($params{mode} =~ /server::instance::enqueue::contention/) || 
        ($params{mode} =~ /server::instance::enqueue::waiting/) ||
        ($params{mode} =~ /server::instance::enqueue::listenqueues/)) {
      # ora11 PE FP TA DL SR TQ KT PW XR SS SJ SQ IT IA UL WP RR KM
      #       PD CF SW CT US TD TK JS FS CN DT TS TT JD SE MW AF TL
      #       PV AS TM TX FB JQ MD TO TH PR RO MR DP WF TB SH RS CU
      #       AE CI PG IS RT HW DR FU
      # ora10 PE FP TA DL SR TQ KT PW XR SS SQ PF IT IA UL WP KM PD
      #       CF SW CT US TD AG JS DT TS TT CN JD SE MW AF TL PV AS
      #       TM FB TX JQ MD TO PR RO MR SK DP WF TB SH RS CU AW CI
      #       PG IS RT HW DR FU
      # ora9  CF CI CU DL DP DR DT DX FB HW IA IS IT JD MD MR PE PF
      #       RO RT SQ SR SS SW TA TD TM TO TS TT TX UL US XR
      my @enqueueresults = $params{handle}->fetchall_array(q{
        SELECT inst_id, eq_type, total_req#, total_wait#, 
            succ_req#, failed_req#, cum_wait_time
        FROM v$enqueue_stat
      });
      foreach (@enqueueresults) {
        my ($inst_id, $name, $total_requests, $total_waits,
          $succeeded_requests, $failed_requests, $cumul_wait_time) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{total_requests} = $total_requests;
        $thisparams{total_waits} = $total_waits;
        $thisparams{succeeded_requests} = $succeeded_requests;
        $thisparams{failed_requests} = $failed_requests;
        $thisparams{cumul_wait_time} = $cumul_wait_time;
        my $enqueue = DBD::Oracle::Server::Instance::Enqueue->new(
            %thisparams);
        add_enqueue($enqueue);
        $num_enqueues++;
      }
      if (! $num_enqueues) {
        $initerrors = 1;
        return undef;
      }
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    total_requests => $params{total_requests},
    total_waits => $params{total_waits},
    succeeded_requests => $params{succeeded_requests},
    failed_requests => $params{failed_requests},
    cumul_wait_time => $params{cumul_wait_time}, # ! milliseconds
    contention => undef,
    percent_waited => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  $self->{name} =~ s/^\s+//;
  $self->{name} =~ s/\s+$//;
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if (($params{mode} =~ /server::instance::enqueue::contention/) ||
      ($params{mode} =~ /server::instance::enqueue::waiting/)) {
    $params{differenciator} = lc $self->{name};
    $self->valdiff(\%params, qw(total_requests total_waits succeeded_requests
        failed_requests cumul_wait_time));
    # enqueue contention
    $self->{contention} = $self->{delta_total_requests} ?
        100 * $self->{delta_total_waits} / $self->{delta_total_requests} : 0;
    # enqueue waiting
    $self->{percent_waited} = ($self->{delta_cumul_wait_time} /
        ($self->{delta_timestamp} * 1000)) * 100;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::enqueue::contention/) {
      $self->add_nagios(
          $self->check_thresholds($self->{contention}, "1", "10"),
          sprintf "enqueue %s %s: %.2f%% of the requests must wait ", 
              $self->{name}, $self->longname(), $self->{contention});
      $self->add_perfdata(sprintf "'%s_contention'=%.2f%%;%s;%s '%s_requests'=%d '%s_waits'=%d",
          $self->{name}, 
          $self->{contention},
          $self->{warningrange}, $self->{criticalrange},
          $self->{name},
          $self->{delta_total_requests},
          $self->{name},
          $self->{delta_total_waits});
    } elsif ($params{mode} =~ /server::instance::enqueue::waiting/) {
      $self->add_nagios(
          # 1 ms wait in 5 minutes
          $self->check_thresholds($self->{percent_waited}, "0.0003333", "0.003333"),
          sprintf "enqueue %s %s: waiting %.4f%% of the time", 
              $self->{name}, $self->longname(), $self->{percent_waited});
      $self->add_perfdata(sprintf "'%s_ms_waited'=%d '%s_pct_waited'=%.4f%%;%s;%s",
          $self->{name}, 
          $self->{delta_cumul_wait_time},
          $self->{name}, 
          $self->{percent_waited},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}


sub longname {
  my $self = shift;
  my $abbrev = <<EOEO;
BL, Buffer Cache Management
BR, Backup/Restore
CF, Controlfile Transaction
CI, Cross-instance Call Invocation
CU, Bind Enqueue
DF, Datafile
DL, Direct Loader Index Creation
DM, Database Mount
DR, Distributed Recovery Process
DX, Distributed Transaction
FP, File Object
FS, File Set
HW, High-Water Lock
IN, Instance Number
IR, Instance Recovery
IS, Instance State
IV, Library Cache Invalidation
JI, Enqueue used during AJV snapshot refresh
JQ, Job Queue
KK, Redo Log "Kick"
KO, Multiple Object Checkpoint
L[A-P], Library Cache Lock
LS, Log Start or Switch
MM, Mount Definition
MR, Media Recovery
N[A-Z], Library Cache Pin
PE, ALTER SYSTEM SET PARAMETER = VALUE
PF, Password File
PI, Parallel Slaves
PR, Process Startup
PS, Parallel Slave Synchronization
Q[A-Z], Row Cache
RO, Object Reuse
RT, Redo Thread
RW, Row Wait
SC, System Commit Number
SM, SMON
SN, Sequence Number
SQ, Sequence Number Enqueue
SR, Synchronized Replication
SS, Sort Segment
ST, Space Management Transaction
SV, Sequence Number Value
TA, Transaction Recovery
TC, Thread Checkpoint
TE, Extend Table
TM, DML Enqueue
TO, Temporary Table Object Enqueue
TS, Temporary Segment (also TableSpace)
TT, Temporary Table
TX, Transaction
UL, User-defined Locks
UN, User Name
US, Undo Segment, Serialization
WL, Being Written Redo Log
XA, Instance Attribute Lock
XI, Instance Registration Lock
EOEO
  my $descriptions = {};
  foreach (split(/\n/, $abbrev)) {
    my ($short, $descr) = split /,/;
    if ($self->{name} =~ /^$short$/) {
      $descr =~ s/^\s+//g;
      return $descr;
    }
  }
  return "";
}


package DBD::Oracle::Server::Instance::Sysstat;

use strict;

our @ISA = qw(DBD::Oracle::Server::Instance);


{
  my @sysstats = ();
  my $initerrors = undef;

  sub add_sysstat {
    push(@sysstats, shift);
  }

  sub return_sysstats {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @sysstats;
  }

  sub init_sysstats {
    my %params = @_;
    my $num_sysstats = 0;
    my %longnames = ();
    if (($params{mode} =~ /server::instance::sysstat::rate/) || 
        ($params{mode} =~ /server::instance::sysstat::listsysstats/)) {
      my @sysstatresults = $params{handle}->fetchall_array(q{
          SELECT statistic#, name, class, value FROM v$sysstat
      });
      foreach (@sysstatresults) {
        my ($number, $name, $class, $value) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if ($params{selectname} && (
              ($params{selectname} !~ /^\d+$/ && (lc $params{selectname} ne lc $name)) ||
              ($params{selectname} =~ /^\d+$/ && ($params{selectname} != $number))));
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{number} = $number;
        $thisparams{class} = $class;
        $thisparams{value} = $value;
        my $sysstat = DBD::Oracle::Server::Instance::Sysstat->new(
            %thisparams);
        add_sysstat($sysstat);
        $num_sysstats++;
      }
      if (! $num_sysstats) {
        $initerrors = 1;
        return undef;
      }
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    number => $params{number},
    class => $params{class},
    value => $params{value},
    rate => $params{rate},
    count => $params{count},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  #$self->{name} =~ s/^\s+//;
  #$self->{name} =~ s/\s+$//;
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sysstat::rate/) {
    $params{differenciator} = lc $self->{name};
    $self->valdiff(\%params, qw(value));
    $self->{rate} = $self->{delta_value} / $self->{delta_timestamp};
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::sysstat::rate/) {
      $self->add_nagios(
          $self->check_thresholds($self->{rate}, "10", "100"),
          sprintf "%.6f %s/sec", $self->{rate}, $self->{name});
      $self->add_perfdata(sprintf "'%s_per_sec'=%.6f;%s;%s",
          $self->{name}, 
          $self->{rate},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "'%s'=%u",
          $self->{name}, 
          $self->{delta_value});
    }
  }
}




package DBD::Oracle::Server::Instance;

use strict;

our @ISA = qw(DBD::Oracle::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    sga => undef,
    processes => {},
    events => [],
    enqueues => [],
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga/) {
    $self->{sga} = DBD::Oracle::Server::Instance::SGA->new(%params);
  } elsif ($params{mode} =~ /server::instance::pga/) {
    $self->{pga} = DBD::Oracle::Server::Instance::PGA->new(%params);
  } elsif ($params{mode} =~ /server::instance::sysstat/) {
    DBD::Oracle::Server::Instance::Sysstat::init_sysstats(%params);
    if (my @sysstats =
        DBD::Oracle::Server::Instance::Sysstat::return_sysstats(%params)) {
      $self->{sysstats} = \@sysstats;
    } else {
      $self->add_nagios_critical("unable to aquire sysstats info");
    }
  } elsif ($params{mode} =~ /server::instance::event/) {
    DBD::Oracle::Server::Instance::Event::init_events(%params);
    if (my @events =
        DBD::Oracle::Server::Instance::Event::return_events(%params)) {
      $self->{events} = \@events;
    } else {
      $self->add_nagios_critical("unable to aquire event info");
    }
  } elsif ($params{mode} =~ /server::instance::enqueue/) {
    DBD::Oracle::Server::Instance::Enqueue::init_enqueues(%params);
    if (my @enqueues =
        DBD::Oracle::Server::Instance::Enqueue::return_enqueues(%params)) {
      $self->{enqueues} = \@enqueues;
    } else {
      $self->add_nagios_critical("unable to aquire enqueue info");
    }
  } elsif ($params{mode} =~ /server::instance::connectedusers/) {
    $self->{connected_users} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM v$session WHERE type = 'USER' 
    });
  } elsif ($params{mode} =~ /server::instance::rman::backup::problems/) {
    $self->{rman_backup_problems} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM v$rman_status 
        WHERE
          operation = 'BACKUP'
        AND
          status != 'COMPLETED'
        AND          
          status != 'RUNNING' 
        AND
          start_time > sysdate-3
    });
  } elsif ($params{mode} =~ /server::instance::sessionusage/) {
    $self->{session_usage} = $self->{handle}->fetchrow_array(q{
        SELECT current_utilization/limit_value*100 
        FROM v$resource_limit WHERE resource_name = 'sessions'
        -- FROM v$resource_limit WHERE resource_name LIKE '%sessions%'
    });
  } elsif ($params{mode} =~ /server::instance::processusage/) {
    $self->{process_usage} = $self->{handle}->fetchrow_array(q{
        SELECT current_utilization/limit_value*100 
        FROM v$resource_limit WHERE resource_name LIKE '%processes%'
    });
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if ($params{mode} =~ /server::instance::sga/) {
    $self->{sga}->nagios(%params);
    $self->merge_nagios($self->{sga});
  } elsif ($params{mode} =~ /server::instance::pga/) {
    $self->{pga}->nagios(%params);
    $self->merge_nagios($self->{pga});
  } elsif ($params{mode} =~ /server::instance::event::listevents/) {
    foreach (sort { $a->{name} cmp $b->{name} } @{$self->{events}}) {
      printf "%10u%s %s %s\n", $_->{event_id}, $_->{idle} ? '*' : '', $_->{shortname}, $_->{name};
    }
    $self->add_nagios_ok("have fun");
  } elsif ($params{mode} =~ /server::instance::event/) {
    foreach (@{$self->{events}}) {
      $_->nagios(%params);
      $self->merge_nagios($_);
    }
    if (! $self->{nagios_level} && ! $params{selectname}) {
      $self->add_nagios_ok("no wait problems");
    }
  } elsif ($params{mode} =~ /server::instance::sysstat::listsysstat/) {
    foreach (sort { $a->{name} cmp $b->{name} } @{$self->{sysstats}}) {
      printf "%10d %s\n", $_->{number}, $_->{name};
    }
    $self->add_nagios_ok("have fun");
  } elsif ($params{mode} =~ /server::instance::sysstat/) {
    foreach (@{$self->{sysstats}}) {
      $_->nagios(%params);
      $self->merge_nagios($_);
    }
    if (! $self->{nagios_level} && ! $params{selectname}) {
      $self->add_nagios_ok("no wait problems");
    }
  } elsif ($params{mode} =~ /server::instance::enqueue::listenqueues/) {
    foreach (sort { $a->{name} cmp $b->{name} } @{$self->{enqueues}}) {
      printf "%s\n", $_->{name};
    }
    $self->add_nagios_ok("have fun");
  } elsif ($params{mode} =~ /server::instance::enqueue/) {
    foreach (@{$self->{enqueues}}) {
      $_->nagios(%params);
      $self->merge_nagios($_);
    }
    if (! $self->{nagios_level} && ! $params{selectname}) {
      $self->add_nagios_ok("no enqueue problem");
    }
  } elsif ($params{mode} =~ /server::instance::connectedusers/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connected_users}, 50, 100),
          sprintf "%d connected users",
              $self->{connected_users});
      $self->add_perfdata(sprintf "connected_users=%d;%d;%d",
          $self->{connected_users},
          $self->{warningrange}, $self->{criticalrange});
  } elsif ($params{mode} =~ /server::instance::rman::backup::problems/) {
      $self->add_nagios(
          $self->check_thresholds($self->{rman_backup_problems}, 1, 2),
          sprintf "rman had %d problems during the last 3 days",
              $self->{rman_backup_problems});
      $self->add_perfdata(sprintf "rman_backup_problems=%d;%d;%d",
          $self->{rman_backup_problems},
          $self->{warningrange}, $self->{criticalrange});
  } elsif ($params{mode} =~ /server::instance::sessionusage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{session_usage}, 80, 100),
          sprintf "%.2f%% of session resources used",
              $self->{session_usage});
      $self->add_perfdata(sprintf "session_usage=%.2f%%;%d;%d",
          $self->{session_usage},
          $self->{warningrange}, $self->{criticalrange});
  } elsif ($params{mode} =~ /server::instance::processusage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{process_usage}, 80, 100),
          sprintf "%.2f%% of process resources used",
              $self->{process_usage});
      $self->add_perfdata(sprintf "process_usage=%.2f%%;%d;%d",
          $self->{process_usage},
          $self->{warningrange}, $self->{criticalrange});
  }
}



package DBD::Oracle::Server::Database::User;

use strict;

our @ISA = qw(DBD::Oracle::Server::Database);


{
  my @users = ();
  my $initerrors = undef;

  sub add_user {
    push(@users, shift);
  }

  sub return_users {
    return reverse
        sort { $a->{name} cmp $b->{name} } @users;
  }

  sub init_users {
    my %params = @_;
    my $num_users = 0;
    if (($params{mode} =~ /server::database::expiredpw/)) {
      my @pwresult = $params{handle}->fetchall_array(q{
          SELECT
              username, expiry_date - sysdate, account_status
          FROM
              dba_users
      });
      foreach (@pwresult) {
        my ($name, $valid_days, $status) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{valid_days} = $valid_days;
        $thisparams{status} = $status;
        my $user = DBD::Oracle::Server::Database::User->new(
            %thisparams);
        add_user($user);
        $num_users++;
      }
      if (! $num_users) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    name => $params{name},
    valid_days => $params{valid_days},
    status => $params{status},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  $self->init_nagios();
}

sub nagios {
  my $self = shift;
  if ($self->{status} eq "EXPIRED") {
    $self->add_nagios_critical(sprintf "password of user %s has expired",
        $self->{name});
  } elsif ($self->{status} eq "EXPIRED (GRACE)") {
    $self->add_nagios_warning(sprintf "password of user %s soon expires",
        $self->{name});
  } elsif ($self->{status} eq "LOCKED (TIMED)") {
    $self->add_nagios_warning(sprintf "user %s is temporarily locked",
        $self->{name});
  } elsif ($self->{status} eq "LOCKED") {
    $self->add_nagios_critical(sprintf "user %s is locked",
        $self->{name});
  } elsif ($self->{status} eq "EXPIRED & LOCKED(TIMED)") {
    $self->add_nagios_critical(sprintf "password of user %s has expired and is temporarily locked",
        $self->{name});
  } elsif ($self->{status} eq "EXPIRED(GRACE) & LOCKED(TIMED)") {
    $self->add_nagios_warning(sprintf "password of user %s soon expires and is temporarily locked",
        $self->{name});
  } elsif ($self->{status} eq "EXPIRED & LOCKED") {
    $self->add_nagios_critical(sprintf "password of user %s has expired and is locked",
        $self->{name});
  } elsif ($self->{status} eq "EXPIRED(GRACE) & LOCKED") {
    $self->add_nagios_critical(sprintf "password of user %s soon expires and is locked",
        $self->{name});
  }
  if ($self->{status} eq "OPEN") {
    if (defined $self->{valid_days}) {
      $self->add_nagios(
          $self->check_thresholds($self->{valid_days}, "7:", "3:"),
          sprintf("password of user %s will expire in %d days",
              $self->{name}, $self->{valid_days}));
      $self->add_perfdata(sprintf "\'pw_%s_valid\'=%.2f;%s;%s",
          lc $self->{name}, $self->{valid_days},
          $self->{warningrange}, $self->{criticalrange});
    } else {
      $self->add_nagios_ok(sprintf "password of user %s will never expire",
          $self->{name});
      $self->add_perfdata(sprintf "\'pw_%s_valid\'=0;0;0",
          lc $self->{name});
    }
  }
}






package DBD::Oracle::Server::Database::FlashRecoveryArea;

use strict;

our @ISA = qw(DBD::Oracle::Server::Database);


{
  my @flash_recovery_areas = ();
  my $initerrors = undef;

  sub add_flash_recovery_area {
    push(@flash_recovery_areas, shift);
  }

  sub return_flash_recovery_areas {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @flash_recovery_areas;
  }
  
  sub init_flash_recovery_areas {
    # as far as i understand it, there is only one flra.
    # we use an array here anyway, because the tablespace code can be reused
    my %params = @_;
    my $num_flash_recovery_areas = 0;
    if (($params{mode} =~ /server::database::flash_recovery_area::usage/) ||
        ($params{mode} =~ /server::database::flash_recovery_area::free/) ||
        ($params{mode} =~ /server::database::flash_recovery_area::listflash_recovery_areas/)) {
      my @flash_recovery_arearesult = ();
      if (DBD::Oracle::Server::return_first_server()->version_is_minimum("10.x")) {
        @flash_recovery_arearesult = $params{handle}->fetchall_array(q{
            SELECT
                name, space_limit, space_used, space_reclaimable, number_of_files
            FROM
                v$recovery_file_dest
        });
      } else {
        # no flash before 10.x
      }
      foreach (@flash_recovery_arearesult) {
        my ($name, $space_limit, $space_used, $space_reclaimable,
            $number_of_files) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{space_limit} = $space_limit;
        $thisparams{space_used} = $space_used;
        $thisparams{space_reclaimable} = $space_reclaimable;
        $thisparams{number_of_files} = lc $number_of_files;
        my $flash_recovery_area = DBD::Oracle::Server::Database::FlashRecoveryArea->new(
            %thisparams);
        add_flash_recovery_area($flash_recovery_area);
        $num_flash_recovery_areas++;
      }
      if (! $num_flash_recovery_areas) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    verbose => $params{verbose},
    handle => $params{handle},
    name => $params{name},
    space_limit => $params{space_limit},
    space_used => $params{space_used},
    space_reclaimable => $params{space_reclaimable},
    number_of_files => $params{number_of_files},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::database::flash_recovery_area::(usage|free)/) {
    $self->{percent_used} =
        ($self->{space_used} - $self->{space_reclaimable}) / $self->{space_limit} * 100;
    $self->{percent_free} = 100 - $self->{percent_used};
    $self->{bytes_used} = $self->{space_used} - $self->{space_reclaimable};
    $self->{bytes_free} = $self->{space_limit} - $self->{space_used};
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::flash_recovery_area::usage/) {
      $self->check_thresholds($self->{percent_used}, "90", "98");
      $self->add_nagios(
          $self->check_thresholds($self->{percent_used}, "90", "98"),
                sprintf("flra (%s) usage is %.2f%%",
                    $self->{name}, $self->{percent_used}));
      $self->add_perfdata(sprintf "\'flra_usage_pct\'=%.2f%%;%d;%d",
          $self->{percent_used},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "\'flra_usage\'=%dMB;%d;%d;%d;%d",
          ($self->{space_used} - $self->{space_reclaimable}) / 1048576,
          $self->{warningrange} * $self->{space_limit} / 100 / 1048576,
          $self->{criticalrange} * $self->{space_limit} / 100 / 1048576,
          0, $self->{space_limit} / 1048576);
    } elsif ($params{mode} =~ /server::database::flash_recovery_area::free/) {
      if (($self->{warningrange} && $self->{warningrange} !~ /^\d+:/) || 
          ($self->{criticalrange} && $self->{criticalrange} !~ /^\d+:/)) {
        $self->add_nagios_unknown("you want an alert if free space is _above_ a threshold????");
        return;
      }
      if (! $params{units}) {
        $params{units} = "%";
      }
      $self->{warning_bytes} = 0;
      $self->{critical_bytes} = 0;
      if ($params{units} eq "%") {
        $self->add_nagios(
            $self->check_thresholds($self->{percent_free}, "5:", "2:"),
            sprintf("flra %s has %.2f%% free space left",
                $self->{name}, $self->{percent_free})
        );
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'flra_free_pct\'=%.2f%%;%d:;%d:",
            $self->{percent_free},
            $self->{warningrange}, $self->{criticalrange});
        $self->add_perfdata(sprintf "\'flra_free\'=%dMB;%.2f:;%.2f:;0;%.2f",
            $self->{bytes_free} / 1048576,
            $self->{warningrange} * $self->{space_limit} / 100 / 1048576,
            $self->{criticalrange} * $self->{space_limit} / 100 / 1048576,
            $self->{space_limit} / 1048576);
      } else {
        my $factor = 1024 * 1024; # default MB
        if ($params{units} eq "GB") {
          $factor = 1024 * 1024 * 1024;
        } elsif ($params{units} eq "MB") {
          $factor = 1024 * 1024;
        } elsif ($params{units} eq "KB") {
          $factor = 1024;
        }
        $self->{warningrange} ||= "5:";
        $self->{criticalrange} ||= "2:";
        my $saved_warningrange = $self->{warningrange};
        my $saved_criticalrange = $self->{criticalrange};
        # : entfernen weil gerechnet werden muss
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->{warningrange} = $self->{warningrange} ?
            $self->{warningrange} * $factor : 5 * $factor;
        $self->{criticalrange} = $self->{criticalrange} ?
            $self->{criticalrange} * $factor : 2 * $factor;
        $self->{percent_warning} = 100 * $self->{warningrange} / $self->{space_limit};
        $self->{percent_critical} = 100 * $self->{criticalrange} / $self->{space_limit};
        $self->{warningrange} .= ':';
        $self->{criticalrange} .= ':';
        $self->add_nagios(
            $self->check_thresholds($self->{bytes_free}, "5242880:", "1048576:"),
                sprintf("flra (%s) has %.2f%s free space left", $self->{name},
                    $self->{bytes_free} / $factor, $params{units})
        );
        $self->{warningrange} = $saved_warningrange;
        $self->{criticalrange} = $saved_criticalrange;
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'flra_free_pct\'=%.2f%%;%.2f:;%.2f:",
            $self->{percent_free}, $self->{percent_warning},
            $self->{percent_critical});
        $self->add_perfdata(sprintf "\'flra_free\'=%.2f%s;%.2f:;%.2f:;0;%.2f",
            $self->{bytes_free} / $factor, $params{units},
            $self->{warningrange},
            $self->{criticalrange},
            $self->{space_limit} / $factor);
      }
    }
  }
}


package DBD::Oracle::Server::Database::Tablespace::Datafile;

use strict;
use File::Basename;

our @ISA = qw(DBD::Oracle::Server::Database::Tablespace);


{
  my @datafiles = ();
  my $initerrors = undef;

  sub add_datafile {
    push(@datafiles, shift);
  }

  sub return_datafiles {
    return reverse
        sort { $a->{name} cmp $b->{name} } @datafiles;
  }

  sub clear_datafiles {
    @datafiles = ();
  }

  sub init_datafiles {
    my %params = @_;
    my $num_datafiles = 0;
    if (($params{mode} =~ /server::database::tablespace::datafile::iotraffic/) ||
        ($params{mode} =~ /server::database::tablespace::datafile::listdatafiles/)) {
      # negative values can occur
      # column datafile format a30
      my @datafileresults = $params{handle}->fetchall_array(q{
        SELECT
          name datafile, phyrds reads, phywrts writes
        FROM 
          v$datafile a, v$filestat b 
        WHERE 
          a.file# = b.file#
        UNION
        SELECT
          name datafile, phyrds reads, phywrts writes
        FROM 
          v$tempfile a, v$tempstat b 
        WHERE 
          a.file# = b.file#
      });
      if (DBD::Oracle::Server::return_first_server()->windows_server()) {
        fileparse_set_fstype("MSWin32");
      }
      foreach (@datafileresults) {
        my ($name, $phyrds, $phywrts) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} &&
              (($name !~ /$params{selectname}/) &&
              (basename($name) !~ /$params{selectname}/));
        } else {
          next if $params{selectname} &&
              ((lc $params{selectname} ne lc $name) &&
              (lc $params{selectname} ne lc basename($name)));
        }
        my %thisparams = %params;
        $thisparams{path} = $name;
        $thisparams{name} = basename($name);
        $thisparams{phyrds} = $phyrds;
        $thisparams{phywrts} = $phywrts;
        my $datafile = 
            DBD::Oracle::Server::Database::Tablespace::Datafile->new(
            %thisparams);
        add_datafile($datafile);
        $num_datafiles++;
      }
    } elsif ($params{mode} =~ /server::database::tablespace::iobalance/) {
      my $sql = q{
           -- SELECT REGEXP_REPLACE(file_name,'^.*.\/.*.\/', '') file_name,
           SELECT file_name,
           SUM(phyrds), SUM(phywrts)
           FROM dba_temp_files, v$filestat 
           WHERE tablespace_name = UPPER(?)
           AND file_id=file# GROUP BY tablespace_name, file_name
           UNION
           -- SELECT REGEXP_REPLACE(file_name,'^.*.\/.*.\/', '') file_name,
           SELECT file_name,
           SUM(phyrds), SUM(phywrts)
           FROM dba_data_files, v$filestat 
           WHERE tablespace_name = UPPER(?)
           AND file_id=file# GROUP BY tablespace_name, file_name };
      if (! DBD::Oracle::Server::return_first_server()->version_is_minimum("9.2.0.3")) {
        # bug 2436600
        $sql = q{
           -- SELECT REGEXP_REPLACE(file_name,'^.*.\/.*.\/', '') file_name,
           SELECT file_name,
           SUM(phyrds), SUM(phywrts)
           FROM dba_data_files, v$filestat 
           WHERE tablespace_name = UPPER(?)
           AND file_id=file# GROUP BY tablespace_name, file_name };
      }
      my @datafileresults = $params{handle}->fetchall_array($sql, $params{tablespace}, $params{tablespace});
      if (DBD::Oracle::Server::return_first_server()->windows_server()) {
        fileparse_set_fstype("MSWin32");
      }
      foreach (@datafileresults) {
        my ($name, $phyrds, $phywrts) = @{$_};
        my %thisparams = %params;
        $thisparams{path} = $name;
        $thisparams{name} = basename($name);
        $thisparams{phyrds} = $phyrds;
        $thisparams{phywrts} = $phywrts;
        my $datafile = 
            DBD::Oracle::Server::Database::Tablespace::Datafile->new(
            %thisparams);
        add_datafile($datafile);
        $num_datafiles++;
      }
      if (! $num_datafiles) {
        $initerrors = 1;
        return undef;
      }
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    path => $params{path},
    name => $params{name},
    phyrds => $params{phyrds},
    phywrts => $params{phywrts},
    io_total => undef,
    io_total_per_sec => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init { 
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::database::tablespace::iobalance/) {
    if (! defined $self->{phyrds}) {
      $self->add_nagios_critical(sprintf "unable to read datafile io %s", $@);
    } else {
      $params{differenciator} = $self->{path};
      $self->valdiff(\%params, qw(phyrds phywrts));
      $self->{io_total} = $self->{delta_phyrds} + $self->{delta_phywrts};
    }
  } elsif ($params{mode} =~ /server::database::tablespace::datafile::iotraffic/) {
    if (! defined $self->{phyrds}) {
      $self->add_nagios_critical(sprintf "unable to read datafile io %s", $@);
    } else {
      $params{differenciator} = $self->{path};
      $self->valdiff(\%params, qw(phyrds phywrts));
      $self->{io_total_per_sec} = ($self->{delta_phyrds} + $self->{delta_phywrts}) /
          $self->{delta_timestamp};
    }
  }
}


sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::tablespace::datafile::iotraffic/) {
      $self->add_nagios(
          $self->check_thresholds($self->{io_total_per_sec}, "1000", "5000"),
          sprintf ("%s: %.2f IO Operations per Second", 
              $self->{name}, $self->{io_total_per_sec}));
      $self->add_perfdata(sprintf "'dbf_%s_io_total_per_sec'=%.2f;%d;%d",
          $self->{name}, $self->{io_total_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}

package DBD::Oracle::Server::Database::Tablespace::Segment;

use strict;

our @ISA = qw(DBD::Oracle::Server::Database::Tablespace);


{
  my @segments = ();
  my $initerrors = undef;

  sub add_segment {
    push(@segments, shift);
  }

  sub return_segments {
    return reverse
        sort { $a->{name} cmp $b->{name} } @segments;
  }

  sub clear_segments {
    @segments = ();
  }

  sub init_segments {
    my %params = @_;
    my $num_segments = 0;
    if (($params{mode} =~
        /server::database::tablespace::segment::top10logicalreads/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10physicalreads/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10bufferbusywaits/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10rowlockwaits/)) {
      my %thisparams = %params;
      $thisparams{name} = "dummy_segment";
      my $segment = DBD::Oracle::Server::Database::Tablespace::Segment->new(
          %thisparams);
      add_segment($segment);
      $num_segments++;
    } elsif ($params{mode} =~
        /server::database::tablespace::segment::extendspace/) {
      my @tablespaceresult = $params{handle}->fetchall_array(q{
          SELECT
              -- tablespace, segment, extent
              -- aber dadurch, dass nur das letzte extent selektiert wird
              -- werden praktisch nur tablespace und segmente ausgegeben
              b.tablespace_name "Tablespace",
              b.segment_type "Type",
              SUBSTR(ext.owner||'.'||ext.segment_name,1,50) "Object Name",
              DECODE(freespace.extent_management, 
                'DICTIONARY', DECODE(b.extents, 
                  1, b.next_extent, ext.bytes * (1 + b.pct_increase / 100)),
                  'LOCAL', DECODE(freespace.allocation_type,
                    'UNIFORM', freespace.initial_extent,
                    'SYSTEM', ext.bytes)
              ) "Required Extent",
              freespace.largest "MaxAvail"
          FROM
              -- dba_segments b,
              -- dba_extents ext,
              (
                SELECT
                    owner, segment_type, segment_name, extents, pct_increase,
                    next_extent, tablespace_name
                FROM
                    dba_segments
                WHERE
                    tablespace_name = ?
              ) b,
              (
                SELECT
                    owner, segment_type, segment_name, extent_id, bytes,
                    tablespace_name
                FROM
                    dba_extents
                WHERE
                    tablespace_name = ?
              ) ext,
              (
                -- dictionary/local, uniform/system, initial, next
                -- und der groesste freie extent pro tablespace
                SELECT
                    b.tablespace_name,
                    b.extent_management,
                    b.allocation_type,
                    b.initial_extent,
                    b.next_extent,
                    max(a.bytes) largest
                FROM
                    dba_free_space a,
                    dba_tablespaces b
                WHERE
                    b.tablespace_name = a.tablespace_name
                AND
                    b.status = 'ONLINE'
                GROUP BY
                    b.tablespace_name,
                    b.extent_management,
                    b.allocation_type,
                    b.initial_extent,
                    b.next_extent
              ) freespace
          WHERE
              b.owner = ext.owner
          AND
              b.segment_type = ext.segment_type
          AND
              b.segment_name = ext.segment_name
          AND
              b.tablespace_name = ext.tablespace_name
          AND
              -- so landet nur das jeweils letzte extent im ergebnis
              (b.extents - 1) = ext.extent_id
          AND
              b.tablespace_name = freespace.tablespace_name
          AND
              freespace.tablespace_name = ?
          ORDER BY
              b.tablespace_name,
              b.segment_type,
              b.segment_name
      }, $params{tablespace}, $params{tablespace}, $params{tablespace});
      foreach (@tablespaceresult) {
        my ($tablespace_name, $segment_type, $object_name, 
            $required_for_next_extent, $largest_free) = @{$_};
        my %thisparams = %params;
        $thisparams{name} = $object_name;
        $thisparams{segment_type} = $segment_type;
        $thisparams{required_for_next_extent} = $required_for_next_extent;
        $thisparams{largest_free} = $largest_free;
        my $segment = DBD::Oracle::Server::Database::Tablespace::Segment->new(
            %thisparams);
        add_segment($segment);
        $num_segments++;
      }
    }
    if (! $num_segments) {
      $initerrors = 1;
      return undef;
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    segment_type => $params{segment_type},
    required_for_next_extent => $params{required_for_next_extent},
    largest_free => $params{largest_free},
    num_users_among_top10logicalreads => undef,
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if (($params{mode} =~
      /server::database::tablespace::segment::top10logicalreads/) ||
      ($params{mode} =~
      /server::database::tablespace::segment::top10physicalreads/) ||
      ($params{mode} =~
      /server::database::tablespace::segment::top10bufferbusywaits/) ||
      ($params{mode} =~
      /server::database::tablespace::segment::top10rowlockwaits/)) {
    my $sql;
    my $mode = (split(/::/, $params{mode}))[4];
    ##    -- SELECT owner, object_name, object_type, value, statistic_name
    if (DBD::Oracle::Server::return_first_server()->version_is_minimum("10.x")) {
      # this uses oracle analytic function rank() over (),
      #  needs oracle >= 10.x
      # for more information see: 
      # http://kenntwas.de/2010/linux/monitoring/check_oracle_health-seg-top10-abfragen-verbessern/
      $sql = q{
          SELECT DO.owner,
                 DO.object_name,
                 DO.object_type,
                 SS.VALUE,
                 SS.statistic_name
            FROM dba_objects DO,
                 (SELECT *
                    FROM (SELECT S.OBJ#,
                                 s.VALUE,
                                 s.statistic_name,
                                 RANK () OVER (ORDER BY s.VALUE DESC) rk
                            FROM v$segstat s
                           WHERE s.statistic_name = ?
                                 /* reduce data to significant values */
                                 AND VALUE <> 0)
                   WHERE rk <= 10   /* top 10 */
                                              ) SS
           WHERE DO.object_id = SS.obj#
      };
    } else {
      my $sql = q{
          SELECT COUNT(*)
          FROM (select DO.owner, DO.object_name, DO.object_type, SS.value,
              SS.statistic_name, row_number () over (order by value desc) RN
              FROM dba_objects DO, v$segstat SS
              WHERE DO.object_id = SS.obj#
              AND statistic_name = ?)
         WHERE RN <= 10
         AND owner not in
             ('CTXSYS', 'DBSNMP', 'MDDATA', 'MDSYS', 'DMSYS', 'OLAPSYS',
             'ORDPLUGINS', 'ORDSYS', 'OUTLN', 'SI_INFORMTN_SCHEMA',
             'SYS', 'SYSMAN', 'SYSTEM')
      };
      # this is a very heavy operation and de-selecting system users
      # makes it even slower, so we fetch all data and do the filtering
      # later in perl.
      $sql = q{
          select DO.owner, DO.object_name, DO.object_type, SS.value,
              SS.statistic_name
              FROM dba_objects DO, v$segstat SS
              WHERE DO.object_id = SS.obj#
              AND statistic_name = ?
      };
    }
    my $statname = {
      top10logicalreads => "logical reads",
      top10physicalreads => "physical reads",
      top10bufferbusywaits => "buffer busy waits",
      top10rowlockwaits => "row lock waits",
    }->{$mode};
    #$self->{"num_users_among_".$mode} =
    #    $self->{handle}->fetchrow_array($sql, $statname);
    # faster version
    # fetch everything
    my @sortedsessions = reverse sort { $a->[3] <=> $b->[3] } $self->{handle}->fetchall_array($sql, $statname);
    if (scalar(@sortedsessions) > 10) {
      @sortedsessions = (@sortedsessions)[0..9];
    }
    my @usersessions = map { $_->[0] !~ /^(CTXSYS|DBSNMP|MDDATA|MDSYS|DMSYS|OLAPSYS|ORDPLUGINS|ORDSYS|OUTLN|SI_INFORMTN_SCHEMA|SYS|SYSMAN|SYSTEM)$/ ? $_ : () } @sortedsessions;
    $self->{"num_users_among_".$mode} = scalar(@usersessions);
    if (scalar(@sortedsessions) == 0) {
    #if (! defined $self->{"num_users_among_".$mode}) {
      $self->add_nagios_critical(sprintf "unable to read top10: %s", $@);
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if (($params{mode} =~
        /server::database::tablespace::segment::top10logicalreads/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10physicalreads/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10bufferbusywaits/) ||
        ($params{mode} =~
        /server::database::tablespace::segment::top10rowlockwaits/)) {
      my $mode = (split(/::/, $params{mode}))[4];
      my $statname = {
        top10logicalreads => "logical reads",
        top10physicalreads => "physical reads",
        top10bufferbusywaits => "buffer busy waits",
        top10rowlockwaits => "row lock waits",
      }->{$mode};
      $self->add_nagios(
          $self->check_thresholds(
              $self->{"num_users_among_".$mode}, "1", "9"),
          sprintf "%d user processes among the top10 %s",
              $self->{"num_users_among_".$mode}, $statname);
      $statname =~ s/\s/_/g;
      $self->add_perfdata(sprintf "users_among_top10_%s=%d;%d;%d",
          $statname, $self->{"num_users_among_".$mode},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~
        /server::database::tablespace::segment::extendspace/) {
      if ($self->{required_for_next_extent} > $self->{largest_free}) {
        $self->add_nagios_critical(
            sprintf "segment %s cannot extend", $self->{name});
      }
    }
  }
}


package DBD::Oracle::Server::Database::Tablespace;

use strict;

our @ISA = qw(DBD::Oracle::Server::Database);


{
  my @tablespaces = ();
  my $initerrors = undef;

  sub add_tablespace {
    push(@tablespaces, shift);
  }

  sub return_tablespaces {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @tablespaces;
  }
  
  sub init_tablespaces {
    my %params = @_;
    my $num_tablespaces = 0;
    if (($params{mode} =~ /server::database::tablespace::usage/) ||
        ($params{mode} =~ /server::database::tablespace::free/) ||
        ($params{mode} =~ /server::database::tablespace::remainingfreetime/) ||
        ($params{mode} =~ /server::database::tablespace::listtablespaces/)) {
      my @tablespaceresult = ();
      if (DBD::Oracle::Server::return_first_server()->version_is_minimum("9.x")) {
        @tablespaceresult = $params{handle}->fetchall_array(q{
            SELECT
                a.tablespace_name         "Tablespace",
                b.status                  "Status",
                b.contents                "Type",
                b.extent_management       "Extent Mgmt",
                a.bytes                   bytes,
                a.maxbytes                bytes_max,
                c.bytes_free + NVL(d.bytes_expired,0)             bytes_free
            FROM
              (
                -- belegter und maximal verfuegbarer platz pro datafile
                -- nach tablespacenamen zusammengefasst
                -- => bytes
                -- => maxbytes
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes)          bytes,
                    SUM(DECODE(a.autoextensible, 'YES', a.maxbytes, 'NO', a.bytes)) maxbytes
                FROM
                    dba_data_files a
                GROUP BY
                    tablespace_name
              ) a,
              sys.dba_tablespaces b,
              (
                -- freier platz pro tablespace
                -- => bytes_free
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes) bytes_free
                FROM
                    dba_free_space a
                GROUP BY
                    tablespace_name
              ) c,
              (
                -- freier platz durch expired extents 
                -- speziell fuer undo tablespaces
                -- => bytes_expired
                SELECT 
                    a.tablespace_name,
                    SUM(a.bytes) bytes_expired
                FROM
                    dba_undo_extents a
                WHERE
                    status = 'EXPIRED' 
                GROUP BY
                    tablespace_name
              ) d
            WHERE
                a.tablespace_name = c.tablespace_name (+)
                AND a.tablespace_name = b.tablespace_name
                AND a.tablespace_name = d.tablespace_name (+)
            UNION ALL
            SELECT
                d.tablespace_name "Tablespace",
                b.status "Status",
                b.contents "Type",
                b.extent_management "Extent Mgmt",
                sum(a.bytes_free + a.bytes_used) bytes,   -- allocated
                SUM(DECODE(d.autoextensible, 'YES', d.maxbytes, 'NO', d.bytes)) bytes_max,
                SUM(a.bytes_free + a.bytes_used - NVL(c.bytes_used, 0)) bytes_free
            FROM
                sys.v_$TEMP_SPACE_HEADER a,
                sys.dba_tablespaces b,
                sys.v_$Temp_extent_pool c,
                dba_temp_files d
            WHERE
                c.file_id(+)             = a.file_id
                and c.tablespace_name(+) = a.tablespace_name
                and d.file_id            = a.file_id
                and d.tablespace_name    = a.tablespace_name
                and b.tablespace_name    = a.tablespace_name
            GROUP BY
                b.status,
                b.contents,
                b.extent_management,
                d.tablespace_name
            ORDER BY
                1
        });
      } elsif (DBD::Oracle::Server::return_first_server()->version_is_minimum("8.x")) {
        @tablespaceresult = $params{handle}->fetchall_array(q{
            SELECT
                a.tablespace_name         "Tablespace",
                b.status                  "Status",
                b.contents                "Type",
                b.extent_management       "Extent Mgmt",
                a.bytes                   bytes,
                a.maxbytes                bytes_max,
                c.bytes_free              bytes_free
            FROM
              (
                -- belegter und maximal verfuegbarer platz pro datafile
                -- nach tablespacenamen zusammengefasst
                -- => bytes
                -- => maxbytes
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes)          bytes,
                    SUM(DECODE(a.autoextensible, 'YES', a.maxbytes, 'NO', a.bytes)) maxbytes
                FROM
                    dba_data_files a
                GROUP BY
                    tablespace_name
              ) a,
              sys.dba_tablespaces b,
              (
                -- freier platz pro tablespace
                -- => bytes_free
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes) bytes_free
                FROM
                    dba_free_space a
                GROUP BY
                    tablespace_name
              ) c
            WHERE
                a.tablespace_name = c.tablespace_name (+)
                AND a.tablespace_name = b.tablespace_name
            UNION ALL
            SELECT
                a.tablespace_name "Tablespace",
                b.status "Status",
                b.contents "Type",
                b.extent_management "Extent Mgmt",
                sum(a.bytes_free + a.bytes_used) bytes,   -- allocated
                d.maxbytes bytes_max,
                SUM(a.bytes_free + a.bytes_used - NVL(c.bytes_used, 0)) bytes_free
            FROM
                sys.v_$TEMP_SPACE_HEADER a,
                sys.dba_tablespaces b,
                sys.v_$Temp_extent_pool c,
                dba_temp_files d
            WHERE
                c.file_id(+)             = a.file_id
                and c.tablespace_name(+) = a.tablespace_name
                and d.file_id            = a.file_id
                and d.tablespace_name    = a.tablespace_name
                and b.tablespace_name    = a.tablespace_name
            GROUP BY
                a.tablespace_name,
                b.status,
                b.contents,
                b.extent_management,
                d.maxbytes
            ORDER BY
                1
        });
      } else {
        @tablespaceresult = $params{handle}->fetchall_array(q{
            SELECT
                a.tablespace_name         "Tablespace",
                b.status                  "Status",
                b.contents                "Type",
                'DICTIONARY'              "Extent Mgmt",
                a.bytes                   bytes,
                a.maxbytes                bytes_max,
                c.bytes_free              bytes_free
            FROM
              (
                -- belegter und maximal verfuegbarer platz pro datafile
                -- nach tablespacenamen zusammengefasst
                -- => bytes
                -- => maxbytes
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes)          bytes,
                    SUM(a.bytes) maxbytes
                FROM
                    dba_data_files a
                GROUP BY
                    tablespace_name
              ) a,
              sys.dba_tablespaces b,
              (
                -- freier platz pro tablespace
                -- => bytes_free
                SELECT
                    a.tablespace_name,
                    SUM(a.bytes) bytes_free
                FROM
                    dba_free_space a
                GROUP BY
                    tablespace_name
              ) c
            WHERE
                a.tablespace_name = c.tablespace_name (+)
                AND a.tablespace_name = b.tablespace_name
        });
      }
      foreach (@tablespaceresult) {
        my ($name, $status, $type, $extentmgmt, $bytes, $bytes_max, $bytes_free) = @{$_};
        next if $params{notemp} && ($type eq "UNDO" || $type eq "TEMPORARY");
        next if $params{noreadonly} && ($status eq "READ ONLY");
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        # host_filesys_pctAvailable
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{bytes} = $bytes;
        $thisparams{bytes_max} = $bytes_max;
        $thisparams{bytes_free} = $bytes_free;
        $thisparams{status} = lc $status;
        $thisparams{type} = lc $type;
        $thisparams{extent_management} = lc $extentmgmt;
        my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::tablespace::fragmentation/) {
      my @tablespaceresult = $params{handle}->fetchall_array(q{
          SELECT
             tablespace_name,       
             COUNT(*) free_chunks,
             DECODE(
              ROUND((max(bytes) / 1024000),2),
              NULL,0,
              ROUND((MAX(bytes) / 1024000),2)) largest_chunk,
             NVL(ROUND(SQRT(MAX(blocks)/SUM(blocks))*(100/SQRT(SQRT(COUNT(blocks)) )),2),
              0) fragmentation_index
          FROM
             sys.dba_free_space 
          GROUP BY 
             tablespace_name
          ORDER BY 
              2 DESC, 1
      });
      foreach (@tablespaceresult) {
        my ($name, $free_chunks, $largest_chunk, $fragmentation_index) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{fsfi} = $fragmentation_index;
        my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::tablespace::segment::top10/) {
      my %thisparams = %params;
      $thisparams{name} = "dummy_segment";
      $thisparams{segments} = [];
      my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
          %thisparams);
      add_tablespace($tablespace);
    } elsif ($params{mode} =~
        /server::database::tablespace::segment::extendspace/) {
      my @tablespaceresult = $params{handle}->fetchall_array(q{
          SELECT
              tablespace_name, extent_management, allocation_type 
          FROM
              dba_tablespaces
      });
      foreach (@tablespaceresult) {
        my ($name, $extent_management, $allocation_type) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{extent_management} = $extent_management;
        $thisparams{allocation_type} = $allocation_type;
        my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::tablespace::datafile/) {
      my %thisparams = %params;
      $thisparams{name} = "dummy_for_datafiles";
      $thisparams{datafiles} = [];
      my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
          %thisparams);
      add_tablespace($tablespace);
    } elsif ($params{mode} =~ /server::database::tablespace::iobalance/) {
      my @tablespaceresult = $params{handle}->fetchall_array(q{
          SELECT tablespace_name FROM dba_tablespaces
      });
      foreach (@tablespaceresult) {
        my ($name) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        my $tablespace = DBD::Oracle::Server::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    verbose => $params{verbose},
    handle => $params{handle},
    name => $params{name},
    bytes => $params{bytes},
    bytes_max => $params{bytes_max},
    bytes_free => $params{bytes_free} || 0,
    extent_management => $params{extent_management},
    type => $params{type},
    status => $params{status},
    fsfi => $params{fsfi},
    segments => [],
    datafiles => [],
    io_total => 0,
    usage_history => [],
    allocation_type => $params{allocation_type},
    largest_free_extent => $params{largest_free_extent},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  $self->set_local_db_thresholds(%params);
  if ($params{mode} =~ /server::database::tablespace::(usage|free)/) {
    if (! defined $self->{bytes_max}) {
      $self->{bytes} = 0;
      $self->{bytes_max} = 0;
      $self->{bytes_free} = 0;
      $self->{percent_used} = 0;
      $self->{real_bytes_max} = $self->{bytes};
      $self->{real_bytes_free} = $self->{bytes_free};
      $self->{percent_as_bar} = '____________________';
    } else {
      if ($params{calcmeth} eq "classic") {
        # (total - free) / total * 100 = % used
        # (used + free - free) / ( used + free)
        if ($self->{bytes_max} == 0) { 
          $self->{percent_used} =
              ($self->{bytes} - $self->{bytes_free}) / $self->{bytes} * 100;
          $self->{real_bytes_max} = $self->{bytes};
          $self->{real_bytes_free} = $self->{bytes_free};
        } elsif ($self->{bytes_max} > $self->{bytes}) {
          $self->{percent_used} =
              ($self->{bytes} - $self->{bytes_free}) / $self->{bytes_max} * 100;
          $self->{real_bytes_max} = $self->{bytes_max};    
          $self->{real_bytes_free} = $self->{bytes_free} + ($self->{bytes_max} - $self->{bytes});
        } else {
          # alter tablespace USERS add datafile 'users02.dbf'
          #     size 5M autoextend on next 200K maxsize 6M;
          # bytes = 5M, maxbytes = 6M
          # ..... data arriving...until ORA-01652: unable to extend temp segment
          # bytes = 6M, maxbytes = 6M
                # alter database datafile 5 resize 8M;
          # bytes = 8M, maxbytes = 6M
          $self->{percent_used} =
              ($self->{bytes} - $self->{bytes_free}) / $self->{bytes} * 100;
          $self->{real_bytes_max} = $self->{bytes};
          $self->{real_bytes_free} = $self->{bytes_free};
        }
      } elsif ($params{calcmeth} eq "vigna") {
      }
    }
    $self->{percent_free} = 100 - $self->{percent_used};
    my $tlen = 20;
    my $len = int((($params{mode} =~ /server::database::tablespace::usage/) ?
        $self->{percent_used} : $self->{percent_free} / 100 * $tlen) + 0.5);
    $self->{percent_as_bar} = '=' x $len . '_' x ($tlen - $len);
  } elsif ($params{mode} =~ /server::database::tablespace::fragmentation/) {
  } elsif ($params{mode} =~ /server::database::tablespace::segment::top10/) {
    DBD::Oracle::Server::Database::Tablespace::Segment::init_segments(%params);
    if (my @segments =
        DBD::Oracle::Server::Database::Tablespace::Segment::return_segments()) {
      $self->{segments} = \@segments;
    } else {
      $self->add_nagios_critical("unable to aquire segment info");
    }
  } elsif ($params{mode} =~ /server::database::tablespace::datafile/) {
    DBD::Oracle::Server::Database::Tablespace::Datafile::init_datafiles(%params);
    if (my @datafiles =
        DBD::Oracle::Server::Database::Tablespace::Datafile::return_datafiles()) {
      $self->{datafiles} = \@datafiles;
    } else {
      $self->add_nagios_critical("unable to aquire datafile info");
    }
  } elsif ($params{mode} =~ /server::database::tablespace::iobalance/) {
    $params{tablespace} = $self->{name};
    DBD::Oracle::Server::Database::Tablespace::Datafile::init_datafiles(%params);
    if (my @datafiles =
        DBD::Oracle::Server::Database::Tablespace::Datafile::return_datafiles()) {
      $self->{datafiles} = \@datafiles;
      map { $self->{io_total} += $_->{io_total} } @datafiles;
      DBD::Oracle::Server::Database::Tablespace::Datafile::clear_datafiles();
    } else {
      $self->add_nagios_critical("unable to aquire datafile info");
    }
  } elsif ($params{mode} =~ /server::database::tablespace::segment::extendspace/) {
    $params{tablespace} = $self->{name};
    DBD::Oracle::Server::Database::Tablespace::Segment::init_segments(%params);
    my @segments =
        DBD::Oracle::Server::Database::Tablespace::Segment::return_segments();
    $self->{segments} = \@segments;
    DBD::Oracle::Server::Database::Tablespace::Segment::clear_segments();
  } elsif ($params{mode} =~ /server::database::tablespace::remainingfreetime/) {
    # load historical data
    # calculate slope, intercept (go back periods * interval)
    # calculate remaining time
    $self->{percent_used} = $self->{bytes_max} == 0 ?
        ($self->{bytes} - $self->{bytes_free}) / $self->{bytes} * 100 :
        ($self->{bytes} - $self->{bytes_free}) / $self->{bytes_max} * 100;
    $self->{usage_history} = $self->load_state( %params ) || [];
    my $now = time;
    my $lookback = ($params{lookback} || 30) * 24 * 3600;
    #$lookback = 91 * 24 * 3600;
    if (scalar(@{$self->{usage_history}})) {
      $self->trace(sprintf "loaded %d data sets from     %s - %s", 
          scalar(@{$self->{usage_history}}),
          scalar localtime((@{$self->{usage_history}})[0]->[0]),
          scalar localtime($now));
      # only data sets with valid usage. only newer than 91 days
      $self->{usage_history} = 
          [ grep { defined $_->[1] && ($now - $_->[0]) < $lookback } @{$self->{usage_history}} ];
      $self->trace(sprintf "trimmed to %d data sets from %s - %s", 
          scalar(@{$self->{usage_history}}),
          scalar localtime((@{$self->{usage_history}})[0]->[0]),
          scalar localtime($now));
    } else {
      $self->trace(sprintf "no historical data found");
    }
    push(@{$self->{usage_history}}, [ time, $self->{percent_used} ]);
    $params{save} = $self->{usage_history};
    $self->save_state(%params);
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::tablespace::usage/) {
      if (! $self->{bytes_max}) {
        $self->check_thresholds($self->{percent_used}, "90", "98");
        if ($self->{status} eq 'offline') {
          $self->add_nagios(
              defined $params{mitigation} ? $params{mitigation} : 1,
              sprintf("tbs %s is offline", $self->{name})
          );
        } else {
          $self->add_nagios(
              defined $params{mitigation} ? $params{mitigation} : 2,
              sprintf("tbs %s has has a problem, maybe needs recovery?", $self->{name})
          );
        }
      } else {
        $self->add_nagios(
            # 'tbs_system_usage_pct'=99.01%;90;98 percent used, warn, crit
            # 'tbs_system_usage'=693MB;630;686;0;700 used, warn, crit, 0, max=total
            $self->check_thresholds($self->{percent_used}, "90", "98"),
            $params{eyecandy} ?
                sprintf("[%s] %s", $self->{percent_as_bar}, $self->{name}) :
                sprintf("tbs %s usage is %.2f%%",
                    $self->{name}, $self->{percent_used})
        );
      }
      $self->add_perfdata(sprintf "\'tbs_%s_usage_pct\'=%.2f%%;%d;%d",
          lc $self->{name},
          $self->{percent_used},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "\'tbs_%s_usage\'=%dMB;%d;%d;%d;%d",
          lc $self->{name},
          ($self->{bytes} - $self->{bytes_free}) / 1048576,
          $self->{warningrange} * $self->{bytes_max} / 100 / 1048576,
          $self->{criticalrange} * $self->{bytes_max} / 100 / 1048576,
          0, $self->{bytes_max} / 1048576);
      $self->add_perfdata(sprintf "\'tbs_%s_alloc\'=%dMB;;;0;%d",
          lc $self->{name},
          $self->{bytes} / 1048576,
          $self->{bytes_max} / 1048576);
    } elsif ($params{mode} =~ /server::database::tablespace::fragmentation/) {
      $self->add_nagios(
          $self->check_thresholds($self->{fsfi}, "30:", "20:"),
          sprintf "tbs %s fsfi is %.2f", $self->{name}, $self->{fsfi});
      $self->add_perfdata(sprintf "\'tbs_%s_fsfi\'=%.2f;%s;%s;0;100",
          lc $self->{name},
          $self->{fsfi},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::database::tablespace::free/) {
      # ->percent_free
      # ->real_bytes_max
      #
      # ausgabe
      #   perfdata tbs_<tbs>_free_pct
      #   perfdata tbs_<tbs>_free        (real_bytes_max - bytes) + bytes_free  (with units)
      #   perfdata tbs_<tbs>_alloc_free  bytes_free (with units)
      #
	      # umrechnen der thresholds
      # ()/%
      # MB
      # GB
      # KB
      if (($self->{warningrange} && $self->{warningrange} !~ /^\d+:/) ||
          ($self->{criticalrange} && $self->{criticalrange} !~ /^\d+:/)) {
        $self->add_nagios_unknown("you want an alert if free space is _above_ a threshold????");
        return;
      }
      if (! $params{units}) {
        $params{units} = "%";
      }
      $self->{warning_bytes} = 0;
      $self->{critical_bytes} = 0;
      if ($params{units} eq "%") {
        if (! $self->{bytes_max}) {
          $self->check_thresholds($self->{percent_used}, "5:", "2:");
          if ($self->{status} eq 'offline') {
            $self->add_nagios(
                defined $params{mitigation} ? $params{mitigation} : 1,
                sprintf("tbs %s is offline", $self->{name})
            );
          } else {
            $self->add_nagios(
                defined $params{mitigation} ? $params{mitigation} : 2,
                sprintf("tbs %s has has a problem, maybe needs recovery?", $self->{name})
            );
          }
          if ($self->{status} eq 'offline') {
            $self->add_nagios_warning(
                sprintf("tbs %s is offline", $self->{name})
            );
          } else {
            $self->add_nagios_critical(
                sprintf("tbs %s has has a problem, maybe needs recovery?", $self->{name}) 
            );
          }
        } else {
          $self->add_nagios(
              $self->check_thresholds($self->{percent_free}, "5:", "2:"),
              sprintf("tbs %s has %.2f%% free space left",
                  $self->{name}, $self->{percent_free})
          );
        }
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'tbs_%s_free_pct\'=%.2f%%;%d:;%d:",
            lc $self->{name},
            $self->{percent_free},
            $self->{warningrange}, $self->{criticalrange});
        $self->add_perfdata(sprintf "\'tbs_%s_free\'=%dMB;%.2f:;%.2f:;0;%.2f",
            lc $self->{name},
            $self->{real_bytes_free} / 1048576,
            $self->{warningrange} * $self->{bytes_max} / 100 / 1048576,
            $self->{criticalrange} * $self->{bytes_max} / 100 / 1048576,
            $self->{real_bytes_max} / 1048576);
      } else {
        my $factor = 1024 * 1024; # default MB
        if ($params{units} eq "GB") {
          $factor = 1024 * 1024 * 1024;
        } elsif ($params{units} eq "MB") {
          $factor = 1024 * 1024;
        } elsif ($params{units} eq "KB") {
          $factor = 1024;
        }
        $self->{warningrange} ||= "5:";
        $self->{criticalrange} ||= "2:";
        my $saved_warningrange = $self->{warningrange};
        my $saved_criticalrange = $self->{criticalrange};
        # : entfernen weil gerechnet werden muss
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->{warningrange} = $self->{warningrange} ?
            $self->{warningrange} * $factor : 5 * $factor;
        $self->{criticalrange} = $self->{criticalrange} ?
            $self->{criticalrange} * $factor : 2 * $factor;
        if (! $self->{bytes_max}) {
          $self->{percent_warning} = 0;
          $self->{percent_critical} = 0;
          $self->{warningrange} .= ':';
          $self->{criticalrange} .= ':';
          $self->check_thresholds($self->{real_bytes_free}, "5242880:", "1048576:");      
          if ($self->{status} eq 'offline') {
            $self->add_nagios(
                defined $params{mitigation} ? $params{mitigation} : 1,
                sprintf("tbs %s is offline", $self->{name})
            );
          } else {
            $self->add_nagios(
                defined $params{mitigation} ? $params{mitigation} : 2,
                sprintf("tbs %s has has a problem, maybe needs recovery?", $self->{name})
            );
          }
        } else {
          $self->{percent_warning} = 100 * $self->{warningrange} / $self->{real_bytes_max};
          $self->{percent_critical} = 100 * $self->{criticalrange} / $self->{real_bytes_max};
          $self->{warningrange} .= ':';
          $self->{criticalrange} .= ':';
          $self->add_nagios(
              $self->check_thresholds($self->{real_bytes_free}, "5242880:", "1048576:"),  
                  sprintf("tbs %s has %.2f%s free space left", $self->{name},
                      $self->{real_bytes_free} / $factor, $params{units})
          );
        }
	$self->{warningrange} = $saved_warningrange;
        $self->{criticalrange} = $saved_criticalrange;
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'tbs_%s_free_pct\'=%.2f%%;%.2f:;%.2f:",
            lc $self->{name},
            $self->{percent_free}, $self->{percent_warning}, 
            $self->{percent_critical});
        $self->add_perfdata(sprintf "\'tbs_%s_free\'=%.2f%s;%.2f:;%.2f:;0;%.2f",
            lc $self->{name},
            $self->{real_bytes_free} / $factor, $params{units},
            $self->{warningrange},
            $self->{criticalrange},
            $self->{real_bytes_max} / $factor);
      }
    } elsif ($params{mode} =~ /server::database::tablespace::fragmentation/) {
      $self->add_nagios(
          $self->check_thresholds($self->{fsfi}, "30:", "20:"),
          sprintf "tbs %s fsfi is %.2f", $self->{name}, $self->{fsfi});
      $self->add_perfdata(sprintf "\'tbs_%s_fsfi\'=%.2f;%s;%s;0;100",
          lc $self->{name},
          $self->{fsfi},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::database::tablespace::segment::top10/) {
      foreach (@{$self->{segments}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::database::tablespace::datafile::listdatafiles/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{datafiles}}) {
        printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::database::tablespace::datafile/) {
      foreach (@{$self->{datafiles}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::database::tablespace::iobalance/) {
      my $cv = 0;
      if (scalar(@{$self->{datafiles}}) == 0) {
        $self->add_nagios($self->check_thresholds($cv, 50, 100),
            sprintf "%s has no datafiles", $self->{name});
      } elsif (scalar(@{$self->{datafiles}}) == 1) {
        $self->add_nagios($self->check_thresholds($cv, 50, 100),
            sprintf "%s has just 1 datafile", $self->{name});
      } elsif ($self->{io_total} == 0) {
        # nix los
        $self->check_thresholds(0, 50, 100);
        $self->add_nagios_ok(sprintf "%s datafiles io is well balanced",
            $self->{name});
      } else {
        my @unbalanced_datafiles = ();
        my $worstfactor = 0;
        my $level = $ERRORS{OK};
        # http://de.wikipedia.org/wiki/Standardabweichung_der_Grundgesamtheit
        # http://de.wikipedia.org/wiki/Variationskoeffizient
        
        # arithmetisches mittel der stichprobe "x quer"
        my $averagetotal = $self->{io_total} / scalar(@{$self->{datafiles}});

        # standardabweichung
        my $sum = 0;
        foreach (@{$self->{datafiles}}) {
          $sum += ($_->{io_total} - $averagetotal) ** 2;
        }
        my $sx = sqrt ($sum / (scalar(@{$self->{datafiles}}) - 1));

        # relative standardabweichung (%RSD)
        $cv = $sx / $averagetotal * 100;

        # jetzt werden diejenigen datafiles ermittelt, die aus der reihe tanzen
        # wie verhaelt sich ihre differenz zum mittelwert zur standardabweichung
        foreach my $datafile (@{$self->{datafiles}}) {
	  my $delta = abs($datafile->{io_total} - $averagetotal);
          my $factor = $delta / $sx * 100;
          $worstfactor = $factor unless $factor <= $worstfactor;
          if ($self->check_thresholds($factor, 50, 100)) {
            push(@unbalanced_datafiles, $datafile);
          }
        }
        if ($self->check_thresholds($worstfactor, 50, 100)) {
          $self->add_nagios($self->check_thresholds($worstfactor, 50, 100),
              sprintf "%s datafiles %s io unbalanced (%f)", $self->{name},
              join(",", map { $_->{name} } @unbalanced_datafiles), $worstfactor);
        } else {
          $self->add_nagios_ok(sprintf "%s datafiles io is well balanced",
              $self->{name});
        }
      }
      # coefficient of variation (cv)
      $self->add_perfdata(sprintf "\'tbs_%s_io_cv\'=%.2f%%;%.2f;%.2f",
          $self->{name}, $cv,
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ 
        /server::database::tablespace::remainingfreetime/) {
      my $lookback = $params{lookback} || 30;
      my $enoughvalues = 0;
      my $newest = time - $lookback * 24 * 3600; 
      my @tmp = grep { $_->[0] > $newest } @{$self->{usage_history}};
      $self->trace(sprintf "found %d usable data sets since %s",
          scalar(@tmp), scalar localtime($newest));
      if ((scalar(@{$self->{usage_history}}) - scalar(@tmp) > 0) && 
        (scalar(@tmp) >= 2)) {
        # only if more than two values are available
        # only if we have data really reaching back some days
        # predicting with two values from the last hour makes no sense
        $self->{usage_history} = \@tmp;
        my $remaining = 99999;
        my $now = time; # normalisieren, so dass jetzt x=0
        my $n = 0; my $sumx = 0; my $sumx2 = 0; my $sumxy = 0; my $sumy = 0; my $sumy2 = 0; my $m = 0; my $r = 0; 
        my $start_usage = undef;
        my $stop_usage = undef;
        foreach (@{$self->{usage_history}}) {
          next if $_->[0] < $newest;
          $start_usage = $_->[1] if ! defined $start_usage;
          $stop_usage = $_->[1];
          my $x = ($_->[0] - $now) / (24 * 3600);
          my $y = $_->[1];
          $n++;                  # increment number of data points by 1
          $sumx  += $x;          # compute sum of x
          $sumx2 += $x * $x;     # compute sum of x**2 
          $sumxy += $x * $y;     # compute sum of x * y
          $sumy  += $y;          # compute sum of y
          $sumy2 += $y * $y;     # compute sum of y**2 
        }
        # compute slope
        $m = ($n * $sumxy  -  $sumx * $sumy) / ($n * $sumx2 - $sumx ** 2);
        # compute y-intercept
        $b = ($sumy * $sumx2  -  $sumx * $sumxy) / ($n * $sumx2  -  $sumx ** 2);
        # compute correlation coefficient
        #$r = ($sumxy - $sumx * $sumy / $n) / 
        #    sqrt(($sumx2 - ($sumx ** 2)/$n) * ($sumy2 - ($sumy ** 2)/$n));
        $self->debug(sprintf "slope: %f  y-intersect: %f", $m, $b);
        if (abs($m) <= 0.000001) { # $m == 0 does not work even if $m is 0.000000
          $self->add_nagios_ok("tablespace usage is constant");
        } elsif ($m > 0) {
          $remaining = (100 - $b) / $m;
          $self->add_nagios($self->check_thresholds($remaining, "90:", "30:"), 
              sprintf "tablespace %s will be full in %d days",
              $self->{name}, $remaining);
          $self->add_perfdata(sprintf "\'tbs_%s_days_until_full\'=%d;%s;%s",
              lc $self->{name},
              $remaining,
              $self->{warningrange}, $self->{criticalrange});
        } else {
          $self->add_nagios_ok("tablespace usage is decreasing");
        }
      } else {
        $self->add_nagios_ok("no data available for prediction");
      }
    } elsif ($params{mode} =~ 
        /server::database::tablespace::segment::extendspace/) {
      my $segments = 0;
      my @largesegments = ();
      foreach my $segment (@{$self->{segments}}) {
        $segments++;
        $segment->nagios(%params);
        if ($segment->{nagios_level}) {
          push(@largesegments, $segment->{name});
        }
        #$self->merge_nagios($segment);
      }
      if (! $segments) {
        $self->add_nagios_ok(
            sprintf "tablespace %s has no segments", $self->{name});
      } elsif (@largesegments) {
        if ($self->{allocation_type} ne "SYSTEM") {
          $self->add_nagios_critical(
              sprintf "tablespace %s cannot extend segment(s) %s", $self->{name},
              join(", ", @largesegments));
        } else {
          $self->add_nagios_ok(
              sprintf "tablespace %s free extents are large enough (autoallocate)",
              $self->{name});
        }
      } elsif (! $self->{nagios_level}) {
        $self->add_nagios_ok(
            sprintf "tablespace %s free extents are large enough",
            $self->{name});
      }
    } elsif ($params{mode} =~ /server::database::tablespace::datafile/) {
printf "%s\n", $self->dump();
    }
  }
}


package DBD::Oracle::Server::Database;

use strict;

our @ISA = qw(DBD::Oracle::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    invalidobjects => {
        invalid_objects => undef,
        invalid_indexes => undef,
        invalid_ind_partitions => undef,
        invalid_registry_components => undef,
    },
    staleobjects => undef,
    corruptedblocks => undef,
    tablespaces => [],
    num_datafiles => undef,
    num_datafiles_max => undef,
    dbusers => [],
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::database::tablespace/) {
    DBD::Oracle::Server::Database::Tablespace::init_tablespaces(%params);
    if (my @tablespaces = 
        DBD::Oracle::Server::Database::Tablespace::return_tablespaces()) {
      $self->{tablespaces} = \@tablespaces;
    } else {
      $self->add_nagios_critical("unable to aquire tablespace info");
    }
  } elsif ($params{mode} =~ /server::database::flash_recovery_area/) {
    DBD::Oracle::Server::Database::FlashRecoveryArea::init_flash_recovery_areas(%params);
    if (my @flash_recovery_areas = 
        DBD::Oracle::Server::Database::FlashRecoveryArea::return_flash_recovery_areas()) {
      $self->{flash_recovery_areas} = \@flash_recovery_areas;
    } else {
      $self->add_nagios_critical("unable to aquire flash recovery area info");
    }
  } elsif ($params{mode} =~ /server::database::invalidobjects/) {
    $self->init_invalid_objects(%params);
  } elsif ($params{mode} =~ /server::database::stalestats/) {
    $self->init_stale_objects(%params);
  } elsif ($params{mode} =~ /server::database::blockcorruption/) {
    $self->init_corrupted_blocks(%params);
  } elsif ($params{mode} =~ /server::database::datafilesexisting/) {
    $self->{num_datafiles_max} = $self->{handle}->fetchrow_array(q{
        SELECT value FROM v$system_parameter WHERE name  = 'db_files'
    });
    $self->{num_datafiles} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM sys.dba_data_files
    });
    if (! defined $self->{num_datafiles_max} ||
      ! defined $self->{num_datafiles}) {
      $self->add_nagios_critical("unable to get number of datafiles");
    }
  } elsif ($params{mode} =~ /server::database::expiredpw/) {
    DBD::Oracle::Server::Database::User::init_users(%params);
    if (my @users = 
        DBD::Oracle::Server::Database::User::return_users()) {
      $self->{users} = \@users;
    } else {
      $self->add_nagios_critical("unable to aquire user info");
    }
  }
}

sub init_invalid_objects {
  my $self = shift;
  my %params = @_;
  my $invalid_objects = undef;
  my $invalid_indexes = undef;
  my $invalid_ind_partitions = undef;
  my $unrecoverable_datafiles = undef;
  $self->{invalidobjects}->{invalid_objects} =
      $self->{handle}->fetchrow_array(q{
          SELECT COUNT(*) 
          FROM dba_objects 
          WHERE status = 'INVALID' AND object_name NOT LIKE 'BIN$%'
      });
  # should be only N/A or VALID
  $self->{invalidobjects}->{invalid_indexes} =
      $self->{handle}->fetchrow_array(q{
          SELECT COUNT(*) 
          FROM dba_indexes 
          WHERE status <> 'VALID' AND status <> 'N/A'
      });
  # should be only USABLE
  $self->{invalidobjects}->{invalid_ind_partitions} =
      $self->{handle}->fetchrow_array(q{
          SELECT COUNT(*) 
          FROM dba_ind_partitions 
          WHERE status <> 'USABLE' AND status <> 'N/A'
      });
  # should be only VALID
  $self->{invalidobjects}->{invalid_registry_components} =
      $self->{handle}->fetchrow_array(q{
          SELECT COUNT(*) 
          FROM dba_registry
          WHERE status <> 'VALID'
      });
  if (! defined $self->{invalidobjects}->{invalid_objects} ||
      ! defined $self->{invalidobjects}->{invalid_indexes} ||
      ! defined $self->{invalidobjects}->{invalid_registry_components} ||
      ! defined $self->{invalidobjects}->{invalid_ind_partitions}) {
    $self->add_nagios_critical("unable to get invalid objects");
    return undef;
  }
}

sub init_stale_objects {
  my $self = shift;
  my %params = @_;
  if ($self->version_is_minimum("10.x")) {
    $self->{staleobjects} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM sys.dba_tab_statistics WHERE stale_stats = 'YES'
            AND owner NOT IN ('SYS','SYSTEM','EXFSYS','DBSNMP','CTXSYS','DMSYS','MDDATA','MDSYS','OLAPSYS','ORDSYS','TSMSYS','WMSYS')
    });
  } else {
    # oracle9 + sqlplus nix gut
    $self->{handle}->func( 10000, 'dbms_output_enable' );
    $self->{handle}->execute(q{
      DECLARE
        l_objList dbms_stats.objectTab;
      BEGIN
        DBMS_OUTPUT.ENABLE (1000000);
        dbms_stats.gather_database_stats( 
          options => 'LIST STALE',
          objlist => l_objList );
        dbms_output.put_line( l_objList.COUNT);
        -- FOR i IN 1 .. l_objList.COUNT
        -- LOOP
        --  dbms_output.put_line( l_objList(i).objType );
        --  dbms_output.put_line( l_objList(i).objName );
        -- END LOOP;
      END;
    });
    $self->{staleobjects} = $self->{handle}->func( 'dbms_output_get' );
  }
  if (! defined $self->{staleobjects}) {
    $self->add_nagios_critical("unable to get stale objects");
    return undef;
  }
}

sub init_corrupted_blocks {
  my $self = shift;
  my %params = @_;
  $self->{corruptedblocks} = $self->{handle}->fetchrow_array(q{
      SELECT COUNT(*) FROM v$database_block_corruption
  });
  if (! defined $self->{corruptedblocks}) {
    $self->add_nagios_critical("unable to get corrupted blocks");
    return undef;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::tablespace::listtablespaces/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{tablespaces}}) {
	printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::database::tablespace/) {
      foreach (@{$self->{tablespaces}}) {
        # sind hier noch nach pctused sortiert
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::database::tablespace::listflash_recovery_areas/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{flash_recovery_areas}}) {
        printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::database::flash_recovery_area/) {
      foreach (@{$self->{flash_recovery_areas}}) {
        # sind hier noch nach pctused sortiert
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::database::invalidobjects/) {
      my @message = ();
      push(@message, sprintf "%d invalid objects",
          $self->{invalidobjects}->{invalid_objects}) if
          $self->{invalidobjects}->{invalid_objects};
      push(@message, sprintf "%d invalid indexes",
          $self->{invalidobjects}->{invalid_indexes}) if
          $self->{invalidobjects}->{invalid_indexes};
      push(@message, sprintf "%d invalid index partitions",
          $self->{invalidobjects}->{invalid_ind_partitions}) if
          $self->{invalidobjects}->{invalid_ind_partitions};
      push(@message, sprintf "%d invalid registry components",
          $self->{invalidobjects}->{invalid_registry_components}) if
          $self->{invalidobjects}->{invalid_registry_components};
      if (scalar(@message)) {
        $self->add_nagios($self->check_thresholds(
            $self->{invalidobjects}->{invalid_objects} +
            $self->{invalidobjects}->{invalid_indexes} +
            $self->{invalidobjects}->{invalid_registry_components} +
            $self->{invalidobjects}->{invalid_ind_partitions}, 0.1, 0.1),
            join(", ", @message));
      } else {
        $self->add_nagios_ok("no invalid objects found");
      }
      foreach (sort keys %{$self->{invalidobjects}}) {
        $self->add_perfdata(sprintf "%s=%d", $_, $self->{invalidobjects}->{$_});
      }
    } elsif ($params{mode} =~ /server::database::stalestats/) {
      $self->add_nagios(
          $self->check_thresholds($self->{staleobjects}, "10", "100"),
          sprintf "%d objects with stale statistics", $self->{staleobjects});
      $self->add_perfdata(sprintf "stale_stats_objects=%d;%s;%s",
          $self->{staleobjects},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::database::blockcorruption/) {
      $self->add_nagios(
          $self->check_thresholds($self->{corruptedblocks}, "1", "10"),
          sprintf "%d database blocks are corrupted", $self->{corruptedblocks});
      $self->add_perfdata(sprintf "corrupted_blocks=%d;%s;%s",
          $self->{corruptedblocks},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::database::datafilesexisting/) {
        my $datafile_usage = $self->{num_datafiles} / 
            $self->{num_datafiles_max} * 100;
      $self->add_nagios(
          $self->check_thresholds($datafile_usage, "80", "90"),
          sprintf "you have %.2f%% of max possible datafiles (%d of %d max)",
              $datafile_usage, $self->{num_datafiles}, $self->{num_datafiles_max});
      $self->add_perfdata(sprintf "datafiles_pct=%.2f%%;%s;%s",
          $datafile_usage,
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "datafiles_num=%d;%s;%s;0;%d",
          $self->{num_datafiles},
          $self->{num_datafiles_max} / 100 * $self->{warningrange},
          $self->{num_datafiles_max} / 100 * $self->{criticalrange},
          $self->{num_datafiles_max});
    } elsif ($params{mode} =~ /server::database::expiredpw/) {
      foreach (@{$self->{users}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    }
  }
}



package DBD::Oracle::Server;

use strict;
use Time::HiRes;
use IO::File;
use File::Copy 'cp';
use Sys::Hostname;
use Data::Dumper;


{
  our $verbose = 0;
  our $scream = 0; # scream if something is not implemented
  our $access = "dbi"; # how do we access the database. 
  our $my_modules_dyn_dir = ""; # where we look for self-written extensions

  my @servers = ();
  my $initerrors = undef;

  sub add_server {
    push(@servers, shift);
  }

  sub return_servers {
    return @servers;
  }
  
  sub return_first_server() {
    return $servers[0];
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    access => $params{method} || "dbi",
    connect => $params{connect},
    username => $params{username},
    password => $params{password},
    timeout => $params{timeout},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    verbose => $params{verbose},
    ident => $params{ident},
    report => $params{report},
    commit => $params{commit},
    version => 'unknown',
    instance => undef,
    database => undef,
    handle => undef,
  };
  bless $self, $class;
  $self->init_nagios();
  if ($self->dbconnect(%params)) {
    $self->{version} = $self->{handle}->fetchrow_array(
        q{ SELECT version FROM v$instance });
    if (! $self->{version}) {
      # This is left over from a test with oracle 7.
      # Just a reminder that it's basically possible to run this plugin
      # against such a dinosaur. At least tablespace-usage works.
      # The rest of the modes probably won't work. At least i didn't try it.
      # Don't even ask me to have a look at it. You'll have to pay for it.
      # And believe me, upgrading to a recent version of Oracle will be
      # much more cheaper.
      $self->{version} = $self->{handle}->fetchrow_array(
          q{ SELECT version FROM product_component_version 
               WHERE product LIKE '%Server%' });
      $self->{os} = 'Unix';
      $self->{dbuser} = $self->{username};
      $self->{thread} = 1;
      $self->{parallel} = 'no';
    } else {
      ($self->{os}, $self->{dbuser}, $self->{thread}, $self->{parallel}, $self->{instance_name}, $self->{database_name}) = $self->{handle}->fetchrow_array(
          q{ select dbms_utility.port_string,sys_context('userenv', 'session_user'),i.thread#,i.parallel, i.instance_name, d.name FROM dual, v$instance i, v$database d });
      #$self->{os} = $self->{handle}->fetchrow_array(
      #    q{ SELECT dbms_utility.port_string FROM dual });
      #$self->{dbuser} = $self->{handle}->fetchrow_array(
      #    q{ SELECT sys_context('userenv', 'session_user') FROM dual });
      #$self->{thread} = $self->{handle}->fetchrow_array(
      #    q{ SELECT thread# FROM v$instance });
      #$self->{parallel} = $self->{handle}->fetchrow_array(
      #    q{ SELECT parallel FROM v$instance });
      if ($self->{ident}) {
        #$self->{instance_name} = $self->{handle}->fetchrow_array(
        #    q{ SELECT instance_name FROM v$instance });
        #$self->{database_name} = $self->{handle}->fetchrow_array(
        #    q{ SELECT name FROM v$database });
        $self->{identstring} = sprintf "(host: %s inst: %s, db: %s) ",
            hostname(), $self->{instance_name}, $self->{database_name};
      }
    }
    DBD::Oracle::Server::add_server($self);
    $self->init(%params);
  }
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $params{handle} = $self->{handle};
  $self->set_global_db_thresholds(\%params);
  if ($params{mode} =~ /^server::instance/) {
    $self->{instance} = DBD::Oracle::Server::Instance->new(%params);
  } elsif ($params{mode} =~ /^server::database/) {
    $self->{database} = DBD::Oracle::Server::Database->new(%params);
  } elsif ($params{mode} =~ /^server::sqlruntime/) {
    $self->set_local_db_thresholds(%params);
    my $tic = Time::HiRes::time();
      @{$self->{genericsql}} =
          $self->{handle}->fetchrow_array($params{selectname});
    $self->{runtime} = Time::HiRes::time() - $tic;
  } elsif ($params{mode} =~ /^server::sql/) {
    $self->set_local_db_thresholds(%params);
    if ($params{regexp}) {
      # sql output is treated as text
      if ($params{name2} eq $params{name}) {
        $self->add_nagios_unknown(sprintf "where's the regexp????");
      } else {
        $self->{genericsql} =
            $self->{handle}->fetchrow_array($params{selectname});
        if (! defined $self->{genericsql}) {
          $self->add_nagios_unknown(sprintf "got no valid response for %s",
              $params{selectname}.($self->{handle}->{errstr} ?
              " - ".$self->{handle}->{errstr} : ""));
        }
      }
    } else {
      # sql output must be a number (or array of numbers)
      @{$self->{genericsql}} =
          $self->{handle}->fetchrow_array($params{selectname});
      if (! (defined $self->{genericsql} &&
          scalar(@{$self->{genericsql}}) &&
          (scalar(grep { /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/ } @{$self->{genericsql}})) ==
          scalar(@{$self->{genericsql}}))) {
        $self->add_nagios_unknown(sprintf "got no valid response for %s",   
            $params{selectname}.($self->{handle}->{errstr} ?
            " - ".$self->{handle}->{errstr} : ""));
      } else {
        # name2 in array
        # units in array
      }
    }
  } elsif ($params{mode} =~ /^server::connectiontime/) {
    $self->{connection_time} = $self->{tac} - $self->{tic};
  } elsif ($params{mode} =~ /^my::([^:.]+)/) {
    my $class = $1;
    my $loaderror = undef;
    substr($class, 0, 1) = uc substr($class, 0, 1);
    foreach my $libpath (split(":", $DBD::Oracle::Server::my_modules_dyn_dir)) {
      foreach my $extmod (glob $libpath."/CheckOracleHealth*.pm") {
        eval {
          $self->trace(sprintf "loading module %s", $extmod);
          require $extmod;
        };
        if ($@) {
          $loaderror = $extmod;
          $self->trace(sprintf "failed loading module %s: %s", $extmod, $@);
        }
      }
    }
    my $obj = {
        handle => $params{handle},
        warningrange => $params{warningrange},
        criticalrange => $params{criticalrange},
    };
    bless $obj, "My$class";
    $self->{my} = $obj;
    if ($self->{my}->isa("DBD::Oracle::Server")) {
      my $dos_init = $self->can("init");
      my $dos_nagios = $self->can("nagios");
      my $my_init = $self->{my}->can("init");
      my $my_nagios = $self->{my}->can("nagios");
      if ($my_init == $dos_init) {
          $self->add_nagios_unknown(
              sprintf "Class %s needs an init() method", ref($self->{my}));
      } elsif ($my_nagios == $dos_nagios) {
          $self->add_nagios_unknown(
              sprintf "Class %s needs a nagios() method", ref($self->{my}));
      } else {
        $self->{my}->init_nagios(%params);
        $self->{my}->init(%params);
      }
    } else {
      $self->add_nagios_unknown(
          sprintf "Class %s is not a subclass of DBD::Oracle::Server%s", 
              ref($self->{my}),
              $loaderror ? sprintf " (syntax error in %s?)", $loaderror : "" );
    }
  } else {
    printf "broken mode %s\n", $params{mode};
  }
}

sub dump {
  my $self = shift;
  my $message = shift || "";
  printf "%s %s\n", $message, Data::Dumper::Dumper($self);
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /^server::instance/) {
      $self->{instance}->nagios(%params);
      $self->merge_nagios($self->{instance});
    } elsif ($params{mode} =~ /^server::database/) {
      $self->{database}->nagios(%params);
      $self->merge_nagios($self->{database});
    } elsif ($params{mode} =~ /^server::connectiontime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connection_time}, 1, 5),
          sprintf "%.2f seconds to connect as %s",
              $self->{connection_time}, $self->{dbuser}||$self->{username});
      $self->add_perfdata(sprintf "connection_time=%.4f;%d;%d",
          $self->{connection_time},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sqlruntime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{runtime}, 1, 5),
          sprintf "%.2f seconds to execute %s",
              $self->{runtime},
              $params{name2} ? $params{name2} : $params{selectname});
      $self->add_perfdata(sprintf "sql_runtime=%.4f;%d;%d",
          $self->{runtime},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sql/) {
      if ($params{regexp}) {
        if (substr($params{name2}, 0, 1) eq '!') {
          $params{name2} =~ s/^!//;
          if ($self->{genericsql} !~ /$params{name2}/) {
            $self->add_nagios_ok(
                sprintf "output %s does not match pattern %s",
                    $self->{genericsql}, $params{name2}); 
          } else {
            $self->add_nagios_critical(
                sprintf "output %s matches pattern %s",
                    $self->{genericsql}, $params{name2});
          }
        } else {
          if ($self->{genericsql} =~ /$params{name2}/) {
            $self->add_nagios_ok(
                sprintf "output %s matches pattern %s",
                    $self->{genericsql}, $params{name2});
          } else {
            $self->add_nagios_critical(
                sprintf "output %s does not match pattern %s",
                    $self->{genericsql}, $params{name2});
          }
        }
      } else {
        $self->add_nagios(
            # the first item in the list will trigger the threshold values
            $self->check_thresholds($self->{genericsql}[0], 1, 5),
                sprintf "%s: %s%s",
                $params{name2} ? lc $params{name2} : lc $params{selectname},
                # float as float, integers as integers
                join(" ", map {
                    (sprintf("%d", $_) eq $_) ? $_ : sprintf("%f", $_)
                } @{$self->{genericsql}}),
                $params{units} ? $params{units} : "");
        my $i = 0;
        # workaround... getting the column names from the database would be nicer
        my @names2_arr = split(/\s+/, $params{name2});
        foreach my $t (@{$self->{genericsql}}) {
          $self->add_perfdata(sprintf "\'%s\'=%s%s;%s;%s",
              $names2_arr[$i] ? lc $names2_arr[$i] : lc $params{selectname},
              # float as float, integers as integers
              (sprintf("%d", $t) eq $t) ? $t : sprintf("%f", $t),
              $params{units} ? $params{units} : "",
            ($i == 0) ? $self->{warningrange} : "",
              ($i == 0) ? $self->{criticalrange} : ""
          ); 
          $i++;
        }
      }
    } elsif ($params{mode} =~ /^my::([^:.]+)/) {
      $self->{my}->nagios(%params);
      $self->merge_nagios($self->{my});
    }
  }
}


sub init_nagios {
  my $self = shift;
  no strict 'refs';
  if (! ref($self)) {
    my $nagiosvar = $self."::nagios";
    my $nagioslevelvar = $self."::nagios_level";
    $$nagiosvar = {
      messages => {
        0 => [],
        1 => [],
        2 => [],
        3 => [],
      },
      perfdata => [],
    };
    $$nagioslevelvar = $ERRORS{OK},
  } else {
    $self->{nagios} = {
      messages => {
        0 => [],
        1 => [],
        2 => [],
        3 => [],
      },
      perfdata => [],
    };
    $self->{nagios_level} = $ERRORS{OK},
  }
}

sub check_thresholds {
  my $self = shift;
  my $value = shift;
  my $defaultwarningrange = shift;
  my $defaultcriticalrange = shift;
  my $level = $ERRORS{OK};
  $self->{warningrange} = defined $self->{warningrange} ?
      $self->{warningrange} : $defaultwarningrange;
  $self->{criticalrange} = defined $self->{criticalrange} ?
      $self->{criticalrange} : $defaultcriticalrange;
  if ($self->{warningrange} =~ /^(\d+)$/) {
    # warning = 10, warn if > 10 or < 0
    $level = $ERRORS{WARNING} 
        if ($value > $1 || $value < 0);
  } elsif ($self->{warningrange} =~ /^(\d+):$/) {
    # warning = 10:, warn if < 10
    $level = $ERRORS{WARNING} 
        if ($value < $1);
  } elsif ($self->{warningrange} =~ /^~:(\d+)$/) {
    # warning = ~:10, warn if > 10
    $level = $ERRORS{WARNING} 
        if ($value > $1);
  } elsif ($self->{warningrange} =~ /^(\d+):(\d+)$/) {
    # warning = 10:20, warn if < 10 or > 20
    $level = $ERRORS{WARNING} 
        if ($value < $1 || $value > $2);
  } elsif ($self->{warningrange} =~ /^@(\d+):(\d+)$/) {
    # warning = @10:20, warn if >= 10 and <= 20
    $level = $ERRORS{WARNING} 
        if ($value >= $1 && $value <= $2);
  }
  if ($self->{criticalrange} =~ /^(\d+)$/) {
    # critical = 10, crit if > 10 or < 0
    $level = $ERRORS{CRITICAL} 
        if ($value > $1 || $value < 0);
  } elsif ($self->{criticalrange} =~ /^(\d+):$/) {
    # critical = 10:, crit if < 10
    $level = $ERRORS{CRITICAL} 
        if ($value < $1);
  } elsif ($self->{criticalrange} =~ /^~:(\d+)$/) {
    # critical = ~:10, crit if > 10
    $level = $ERRORS{CRITICAL} 
        if ($value > $1);
  } elsif ($self->{criticalrange} =~ /^(\d+):(\d+)$/) {
    # critical = 10:20, crit if < 10 or > 20
    $level = $ERRORS{CRITICAL} 
        if ($value < $1 || $value > $2);
  } elsif ($self->{criticalrange} =~ /^@(\d+):(\d+)$/) {
    # critical = @10:20, crit if >= 10 and <= 20
    $level = $ERRORS{CRITICAL} 
        if ($value >= $1 && $value <= $2);
  }
  return $level;
  #
  # syntax error must be reported with returncode -1
  #
}

sub add_nagios {
  my $self = shift;
  my $level = shift;
  my $message = shift;
  push(@{$self->{nagios}->{messages}->{$level}}, $message);
  # recalc current level
  foreach my $llevel (qw(CRITICAL WARNING UNKNOWN OK)) {
    if (scalar(@{$self->{nagios}->{messages}->{$ERRORS{$llevel}}})) {
      $self->{nagios_level} = $ERRORS{$llevel};
    }
  }
}

sub add_nagios_ok {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{OK}, $message);
}

sub add_nagios_warning {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{WARNING}, $message);
}

sub add_nagios_critical {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{CRITICAL}, $message);
}

sub add_nagios_unknown {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{UNKNOWN}, $message);
}

sub add_perfdata {
  my $self = shift;
  my $data = shift;
  push(@{$self->{nagios}->{perfdata}}, $data);
}

sub merge_nagios {
  my $self = shift;
  my $child = shift;
  foreach my $level (0..3) {
    foreach (@{$child->{nagios}->{messages}->{$level}}) {
      $self->add_nagios($level, $_);
    }
    #push(@{$self->{nagios}->{messages}->{$level}},
    #    @{$child->{nagios}->{messages}->{$level}});
  }
  push(@{$self->{nagios}->{perfdata}}, @{$child->{nagios}->{perfdata}});
}


sub calculate_result {
  my $self = shift;
  my $multiline = 0;
  map {
    $self->{nagios_level} = $ERRORS{$_} if 
        (scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}}));
  } ("OK", "UNKNOWN", "WARNING", "CRITICAL");
  if ($ENV{NRPE_MULTILINESUPPORT} && 
      length join(" ", @{$self->{nagios}->{perfdata}}) > 200) {
    $multiline = 1;
  }
  my $all_messages = join(($multiline ? "\n" : ", "), map {
      join(($multiline ? "\n" : ", "), @{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } grep {
      scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } ("CRITICAL", "WARNING", "UNKNOWN", "OK"));
  my $bad_messages = join(($multiline ? "\n" : ", "), map {
      join(($multiline ? "\n" : ", "), @{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } grep {
      scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } ("CRITICAL", "WARNING", "UNKNOWN"));
  my $all_messages_short = $bad_messages ? $bad_messages : 'no problems';
  my $all_messages_html = "<table style=\"border-collapse: collapse;\">".
      join("", map {
          my $level = $_;
          join("", map {
              sprintf "<tr valign=\"top\"><td class=\"service%s\">%s</td></tr>",
              $level, $_;
          } @{$self->{nagios}->{messages}->{$ERRORS{$_}}});
      } grep {
          scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
      } ("CRITICAL", "WARNING", "UNKNOWN", "OK")).
  "</table>";
  if (exists $self->{identstring}) {
    $self->{nagios_message} .= $self->{identstring};
  }
  if ($self->{report} eq "long") {
    $self->{nagios_message} .= $all_messages;
  } elsif ($self->{report} eq "short") {
    $self->{nagios_message} .= $all_messages_short;
  } elsif ($self->{report} eq "html") {
    $self->{nagios_message} .= $all_messages_short."\n".$all_messages_html;
  }
  $self->{perfdata} = join(" ", @{$self->{nagios}->{perfdata}});
}

sub set_global_db_thresholds {
  my $self = shift;
  my $params = shift;
  my $warning = undef;
  my $critical = undef;
  return unless defined $params->{dbthresholds};
  $params->{name0} = $params->{dbthresholds};
  # :pluginmode   :name     :warning    :critical
  # mode          empty
  #
  eval {
    if ($self->{handle}->fetchrow_array(q{
        SELECT table_name FROM user_tables
        WHERE table_name = 'CHECK_ORACLE_HEALTH_THRESHOLDS'
      })) {
      my @dbthresholds = $self->{handle}->fetchall_array(q{
          SELECT * FROM check_oracle_health_thresholds
      });
      $params->{dbthresholds} = \@dbthresholds;
      foreach (@dbthresholds) {
        if (($_->[0] eq $params->{cmdlinemode}) &&
            (! defined $_->[1] || ! $_->[1])) {
          ($warning, $critical) = ($_->[2], $_->[3]);
        }
      }
    }
  };
  if (! $@) {
    if ($warning) {
      $params->{warningrange} = $warning;
      $self->trace("read warningthreshold %s from database", $warning);
    }
    if ($critical) {
      $params->{criticalrange} = $critical;
      $self->trace("read criticalthreshold %s from database", $critical);
    }
  }
}

sub set_local_db_thresholds {
  my $self = shift;
  my %params = @_;
  my $warning = undef;
  my $critical = undef;
  # :pluginmode   :name     :warning    :critical
  # mode          name0
  # mode          name2
  # mode          name
  #
  # first: argument of --dbthresholds, it it exists
  # second: --name2
  # third: --name
  if (ref($params{dbthresholds}) eq 'ARRAY') {
    my $marker;
    foreach (@{$params{dbthresholds}}) {
      if ($_->[0] eq $params{cmdlinemode}) {
        if (defined $_->[1] && $params{name0} && $_->[1] eq $params{name0}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name0};
          last;
        } elsif (defined $_->[1] && $params{name2} && $_->[1] eq $params{name2}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name2};
          last;
        } elsif (defined $_->[1] && $params{name} && $_->[1] eq $params{name}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name};
          last;
        }
      }
    }
    if ($warning) {
      $self->{warningrange} = $warning;
      $self->trace("read warningthreshold %s for %s from database", 
         $marker, $warning);
    }
    if ($critical) {
      $self->{criticalrange} = $critical;
      $self->trace("read criticalthreshold %s for %s from database", 
          $marker, $critical); 
    }
  }
}

sub debug {
  my $self = shift;
  my $msg = shift;
  if ($DBD::Oracle::Server::verbose) {
    printf "%s %s\n", $msg, ref($self);
  }
}

sub dbconnect {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{tic} = Time::HiRes::time();
  $self->{handle} = DBD::Oracle::Server::Connection->new(%params);
  if ($self->{handle}->{errstr}) {
    if ($params{mode} =~ /^server::tnsping/ &&
        $self->{handle}->{errstr} =~ /ORA-01017/) {
      $self->add_nagios($ERRORS{OK},
          sprintf "connection established to %s.", $self->{connect});
      $retval = undef;
    } elsif ($self->{handle}->{errstr} eq "alarm\n") {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "connection could not be established within %d seconds",
              $self->{timeout});
    } elsif ($self->{handle}->{errstr} =~ /specify/) {
      $self->add_nagios($ERRORS{CRITICAL}, $self->{handle}->{errstr});
      $retval = undef;
    } else {
      if ($self->{connect} =~ /^(.*?)\/(.*)@(.*)$/) {
        $self->{connect} = sprintf "%s/***@%s", $1, $3;
      } elsif ($self->{connect} =~ /^(.*?)@(.*)$/) {
        $self->{connect} = sprintf "%s@%s", $1, $2;
      }
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "cannot connect to %s. %s",
          $self->{connect}, $self->{handle}->{errstr});
      $retval = undef;
    }
  } else {
    $retval = $self->{handle};
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub trace {
  my $self = shift;
  my $format = shift;
  $self->{trace} = -f "/tmp/check_oracle_health.trace" ? 1 : 0;
  if ($self->{verbose}) {
    printf("%s: ", scalar localtime);
    printf($format, @_);
    printf "\n";
  }
  if ($self->{trace}) {
    my $logfh = new IO::File;
    $logfh->autoflush(1);
    if ($logfh->open("/tmp/check_oracle_health.trace", "a")) {
      $logfh->printf("%s: ", scalar localtime);
      $logfh->printf($format, @_);
      $logfh->printf("\n");
      $logfh->close();
    }
  }
}

sub DESTROY {
  my $self = shift;
  my $handle1 = "null";
  my $handle2 = "null";
  if (defined $self->{handle}) {
    $handle1 = ref($self->{handle});
    if (defined $self->{handle}->{handle}) {
      $handle2 = ref($self->{handle}->{handle});
    }
  }
  #$self->trace(sprintf "DESTROY %s with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::Oracle::Server") {
  }
  #$self->trace(sprintf "DESTROY %s exit with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::Oracle::Server") {
    #printf "humpftata\n";
  }
}

sub save_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  my $mode = $params{mode};
  if ($params{connect} && $params{connect} =~ /(\w+)\/(\w+)@(\w+)/) {
    $params{connect} = $3;
  } elsif ($params{connect}) {
    # just to be sure
    $params{connect} =~ s/\//_/g;
  }
  if ($^O =~ /MSWin/) {
    $mode =~ s/::/_/g;
    $params{statefilesdir} = $self->system_vartmpdir();
  }
  if (! -d $params{statefilesdir}) {
    eval {
      use File::Path;
      mkpath $params{statefilesdir};
    };
  }
  if ($@ || ! -w $params{statefilesdir}) {
    $self->add_nagios($ERRORS{CRITICAL},
        sprintf "statefilesdir %s does not exist or is not writable", 
        $params{statefilesdir});
    return;
  }
  my $statefile = sprintf "%s_%s", $params{connect}, $mode;
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{tablespace} ? "_".$params{tablespace} : "";
  $extension .= $params{datafile} ? "_".$params{datafile} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  $statefile = sprintf "%s/%s", $params{statefilesdir}, $statefile;
  if (open(STATE, ">$statefile")) {
    if ((ref($params{save}) eq "HASH") && exists $params{save}->{timestamp}) {
      $params{save}->{localtime} = scalar localtime $params{save}->{timestamp};
    }
    printf STATE Data::Dumper::Dumper($params{save});
    close STATE;
  } else {
    $self->add_nagios($ERRORS{CRITICAL},
        sprintf "statefile %s is not writable", $statefile);
  }
  $self->debug(sprintf "saved %s to %s",
      Data::Dumper::Dumper($params{save}), $statefile);
}

sub load_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  my $mode = $params{mode};
  if ($params{connect} && $params{connect} =~ /(\w+)\/(\w+)@(\w+)/) {
    $params{connect} = $3;
  } elsif ($params{connect}) {
    # just to be sure
    $params{connect} =~ s/\//_/g;
  }
  if ($^O =~ /MSWin/) {
    $mode =~ s/::/_/g;
    $params{statefilesdir} = $self->system_vartmpdir();
  }
  my $statefile = sprintf "%s_%s", $params{connect}, $mode;
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{tablespace} ? "_".$params{tablespace} : "";
  $extension .= $params{datafile} ? "_".$params{datafile} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  $statefile = sprintf "%s/%s", $params{statefilesdir}, $statefile;
  if ( -f $statefile) {
    our $VAR1;
    eval {
      require $statefile;
    };
    if($@) {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "statefile %s is corrupt", $statefile);
    }
    $self->debug(sprintf "load %s", Data::Dumper::Dumper($VAR1));
    return $VAR1;
  } else {
    return undef;
  }
}

sub valdiff {
  my $self = shift;
  my $pparams = shift;
  my %params = %{$pparams};
  my @keys = @_;
  my $last_values = $self->load_state(%params) || eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = 0;
    }
    $empty_events->{timestamp} = 0;
    $empty_events;
  };
  foreach (@keys) {
    $last_values->{$_} = 0 if ! exists $last_values->{$_};
    if ($self->{$_} >= $last_values->{$_}) {
      $self->{'delta_'.$_} = $self->{$_} - $last_values->{$_};
    } else {
      # vermutlich db restart und zaehler alle auf null
      $self->{'delta_'.$_} = $self->{$_};
    }
    $self->debug(sprintf "delta_%s %f", $_, $self->{'delta_'.$_});
  }
  $self->{'delta_timestamp'} = time - $last_values->{timestamp};
  $params{save} = eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = $self->{$_};
    }
    $empty_events->{timestamp} = time;
    $empty_events;
  };
  $self->save_state(%params);
}

sub requires_version {
  my $self = shift;
  my $version = shift;
  my @instances = DBD::Oracle::Server::return_servers();
  my $instversion = $instances[0]->{version};
  if (! $self->version_is_minimum($version)) {
    $self->add_nagios($ERRORS{UNKNOWN}, 
        sprintf "not implemented/possible for Oracle release %s", $instversion);
  }
}

sub version_is_minimum {
  # the current version is newer or equal
  my $self = shift;
  my $version = shift;
  my $newer = 1;
  my @instances = DBD::Oracle::Server::return_servers();
  my @v1 = map { $_ eq "x" ? 0 : $_ } split(/\./, $version);
  my @v2 = split(/\./, $instances[0]->{version});
  if (scalar(@v1) > scalar(@v2)) {
    push(@v2, (0) x (scalar(@v1) - scalar(@v2)));
  } elsif (scalar(@v2) > scalar(@v1)) {
    push(@v1, (0) x (scalar(@v2) - scalar(@v1)));
  }
  foreach my $pos (0..$#v1) {
    if ($v2[$pos] > $v1[$pos]) {
      $newer = 1;
      last;
    } elsif ($v2[$pos] < $v1[$pos]) {
      $newer = 0;
      last;
    }
  }
  #printf STDERR "check if %s os minimum %s\n", join(".", @v2), join(".", @v1);
  return $newer;
}

sub instance_rac {
  my $self = shift;
  my @instances = DBD::Oracle::Server::return_servers();
  return (lc $instances[0]->{parallel} eq "yes") ? 1 : 0;
}

sub instance_thread {
  my $self = shift;
  my @instances = DBD::Oracle::Server::return_servers();
  return $instances[0]->{thread};
}

sub windows_server {
  my $self = shift;
  my @instances = DBD::Oracle::Server::return_servers();
  if ($instances[0]->{os} =~ /Win/i) {
    return 1;
  } else {
    return 0;
  }
}

sub system_vartmpdir {
  my $self = shift;
  if ($^O =~ /MSWin/) {
    return $self->system_tmpdir();
  } else {
    return "/var/tmp/check_oracle_health";
  }
}

sub system_oldvartmpdir {
  my $self = shift;
  return "/tmp";
}

sub system_tmpdir {
  my $self = shift;
  if ($^O =~ /MSWin/) {
    return $ENV{TEMP} if defined $ENV{TEMP};
    return $ENV{TMP} if defined $ENV{TMP};
    return File::Spec->catfile($ENV{windir}, 'Temp')
        if defined $ENV{windir};
    return 'C:\Temp';
  } else {
    return "/tmp";
  }
}


package DBD::Oracle::Server::Connection;

use strict;

our @ISA = qw(DBD::Oracle::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    mode => $params{mode},
    timeout => $params{timeout},
    access => $params{method} || "dbi",
    connect => $params{connect},
    username => $params{username},
    password => $params{password},
    verbose => $params{verbose},
    commit => $params{commit},
    tnsadmin => $ENV{TNS_ADMIN},
    oraclehome => $ENV{ORACLE_HOME},
    handle => undef,
  };
  bless $self, $class;
  if ($params{method} eq "dbi") {
    bless $self, "DBD::Oracle::Server::Connection::Dbi";
  } elsif ($params{method} eq "sqlplus") {
    bless $self, "DBD::Oracle::Server::Connection::Sqlplus";
  } elsif ($params{method} eq "sqlrelay") {
    bless $self, "DBD::Oracle::Server::Connection::Sqlrelay";
  }
  $self->init(%params);
  return $self;
}


package DBD::Oracle::Server::Connection::Dbi;

use strict;
use Net::Ping;

our @ISA = qw(DBD::Oracle::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      $self->{sid} = $self->{connect};
      $self->{username} ||= time;  # prefer an existing user
      $self->{password} ||= time;
    }
  } else {
    if (! $self->{connect} || ! $self->{username} || ! $self->{password}) {
      if ($self->{connect} && $self->{connect} =~ /(\w+)\/(\w+)@([\w\-\.]+)/) {
        $self->{connect} = $3;
        $self->{username} = $1;
        $self->{password} = $2;
        $self->{sid} = $self->{connect};
      } elsif ($self->{connect} && $self->{connect} =~ /^(sys|sysdba)@([\w\-\.]+)/) {
        $self->{connect} = $2;
        $self->{username} = $1;
        $self->{password} = '';
        $self->{sid} = $self->{connect};
      } elsif ($self->{connect} && ! $self->{username} && ! $self->{password}) {
        # maybe this is a os authenticated user
        delete $ENV{TWO_TASK};
        $self->{sid} = $self->{connect};
        if ($^O ne "hpux") {       #hpux && 1.21 only accepts "DBI:Oracle:SID"
          $self->{connect} = "";   #linux 1.20 only accepts "DBI:Oracle:" + ORACLE_SID
        }
        $self->{username} = '/';
        $self->{password} = "";
      } else {
        $self->{errstr} = "Please specify database, username and password";
        return undef;
      }
    } else {
      $self->{sid} = $self->{connect};
    }
  }
  if (! exists $self->{errstr}) {
    $ENV{ORACLE_SID} = $self->{sid};
    eval {
      require DBI;
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      my $dsn = sprintf "DBI:Oracle:%s", $self->{connect};
      my $connecthash = { RaiseError => 0, AutoCommit => $self->{commit}, PrintError => 0 };
      if ($self->{username} eq "sys" || $self->{username} eq "sysdba") {
        $connecthash = { RaiseError => 0, AutoCommit => $self->{commit}, PrintError => 0,
              #ora_session_mode => DBD::Oracle::ORA_SYSDBA   
              ora_session_mode => 0x0002  };
        $dsn = sprintf "DBI:Oracle:";
      }
      if ($self->{handle} = DBI->connect(
          $dsn,
          $self->{username},
          $self->{password},
          $connecthash)) {
        $self->{handle}->do(q{
            ALTER SESSION SET NLS_NUMERIC_CHARACTERS=".," });
        $retval = $self;
        if ($self->{mode} =~ /^server::tnsping/) {
          $self->{handle}->disconnect();
          die "ORA-01017"; # fake a successful connect with wrong password
        }
      } else {
        $self->{errstr} = DBI::errstr();
      }
    };
    if ($@) {
      $self->{errstr} = $@;
      $retval = undef;
    }
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub tnsping {
  my $self = shift;
  my $retval = undef;
  if ($self->{tnsadmin}) {
    $self->{tnsfile} = $self->{tnsadmin}.'/tnanames.ora';
  } elsif ($self->{oraclehome}) {
    $self->{tnsfile} = $self->{oraclehome}.'/network/admin/tnsnames.ora';
  } else {
    $self->{tnsfile} = $ENV{HOME}.'/tnsnames.ora';
  }
  my $re_blank      = '^$';
  my $re_comment    = '^#';
  my $re_tns_sentry = '^'.$self->{sid}.'.*?=';                 # specific entry
  my $re_tns_gentry = '^\w.*?=';                    # generic entry
  my $re_tns_pair   = '\w+\s*\=\s*[\w\.]+';         # used to parse key=val
  my $re_keys       = 'host|port|sid';
  my @tnsfile = split(/\n/, do { local (@ARGV, $/) = $self->{tnsfile}; my $x = <>; close ARGV; $x; });
  my $found = 0;
  my $datastring = "";
  foreach (@tnsfile) {
    chomp;
    next if /$re_blank/;
    next if /$re_comment/;
    if (/$re_tns_sentry/) {
      $found = 1;
      s/$re_tns_sentry//;
      $datastring = $_;
    }
    if ($found) {
      last if /$re_tns_gentry/;
      $datastring .= $_;
    }
  }
  if ($found) {
    while ($datastring =~ m/($re_tns_pair)/g) {
      my ($key, $value) = split(/=/, $1);
      $key =~ s/^\s+//g;
      $value =~ s/^\s+//g;
      $key =~ s/\s+$//g;
      $value =~ s/\s+$//g;
      next unless $key =~ /$re_keys/i;
      if (lc $key eq "host") {
        $self->{hostname} = $value;
      } elsif (lc $key eq "port") {
        $self->{port} = $value;
      }
    }
  }
  if (exists $self->{hostname} && exists $self->{port}) {
    my $p = Net::Ping->new("tcp");
    $p->{port_num} = $self->{port};
    if ($p->ping($self->{hostname}, $self->{timeout} - 1)) {
      $self->{handle} = 1;
      $retval = $self;
    } else {
      $self->{errstr} = sprintf "tnsping timed out after %d seconds",
          $self->{timeout};
    }
  } else {
    $self->{errstr} = sprintf "could not find host and address for %s",
        $self->{sid};
  }
  return $retval;
}

sub fetchrow_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my @row = ();
  $self->trace(sprintf "fetchrow_array: %s", $sql);
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    @row = $sth->fetchrow_array();
  }; 
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
    $self->debug(sprintf "bumm %s", $self->{handle}->errstr());
    $self->{errstr} = $self->{handle}->errstr();
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper(\@row));
  if (-f "/tmp/check_oracle_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_oracle_health_simulation/".$self->{mode}; <> };
    @row = split(/\s+/, (split(/\n/, $simulation))[0]);
  }
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  $self->trace(sprintf "fetchall_array: %s", $sql);
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    $rows = $sth->fetchall_arrayref();
  }; 
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper($rows));
  if (-f "/tmp/check_oracle_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_oracle_health_simulation/".$self->{mode}; <> };
    @{$rows} = map { [ split(/\s+/, $_) ] } split(/\n/, $simulation);
  }
  return @{$rows};
}

sub func {
  my $self = shift;
  $self->{handle}->func(@_);
}


sub execute {
  my $self = shift;
  my $sql = shift;
  eval {
    my $sth = $self->{handle}->prepare($sql);
    $sth->execute();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
}

sub DESTROY {
  my $self = shift;
  $self->trace(sprintf "disconnecting DBD %s",
      $self->{handle} ? "with handle" : "without handle");
  $self->{handle}->disconnect() if $self->{handle};
}

package DBD::Oracle::Server::Connection::Sqlplus;

use strict;
use File::Temp qw/tempfile/;
use File::Basename qw/dirname/;

our @ISA = qw(DBD::Oracle::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{loginstring} = "traditional";
  my $template = $self->{mode}.'XXXXX';
  my $now = time;
  if ($^O =~ /MSWin/) {
    $template =~ s/::/_/g;
    # This is no longer necessary. (Explanation in fetchrow_array "best practice".
    # But maybe we have crap files for whatever reason.
    my $pattern = $template;
    $pattern =~ s/XXXXX$//g;
    foreach (glob $self->system_tmpdir().'/'.$pattern.'*') {
      if (/\.(sql|out|err)$/) {
        if (($now - (stat $_)[9]) > 300) {
          unlink $_;
        }
      }
    }
  }
  eval {
    my ($tempfile_handle, $tempfile) =
        tempfile($template, SUFFIX => ".temp", UNLINK => 1,
        DIR => $self->system_tmpdir() );
    close $tempfile_handle;
    ($self->{sql_commandfile} = $tempfile) =~ s/temp$/sql/;
    ($self->{sql_resultfile} = $tempfile) =~ s/temp$/out/;
    ($self->{sql_outfile} = $tempfile) =~ s/temp$/err/;
  };
  if ($@) {
    $self->{errstr} = sprintf "cannot create a temporary file in %s",
        $self->system_tmpdir();
  }
  if ($self->{mode} =~ /^server::tnsping/) {
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      $self->{sid} = $self->{connect};
      $self->{username} ||= time;  # prefer an existing user
      $self->{password} = time;
    }
  } else {
    if ($self->{connect} && ! $self->{username} && ! $self->{password} &&
        $self->{connect} =~ /(\w+)\/(\w+)@([\w\-\._]+)/) {
      # --connect nagios/oradbmon@bba
      $self->{connect} = $3;
      $self->{username} = $1;
      $self->{password} = $2;
      $self->{sid} = $self->{connect};
      if ($self->{username} eq "sys") {
        delete $ENV{TWO_TASK};
        $self->{loginstring} = "sys";
      } else {
        $self->{loginstring} = "traditional";
      }
    } elsif ($self->{connect} && ! $self->{username} && ! $self->{password} &&
        $self->{connect} =~ /sysdba@([\w\-\._]+)/) {
      # --connect sysdba@bba
      $self->{connect} = $1;
      $self->{username} = "/";
      $self->{sid} = $self->{connect};
      $self->{loginstring} = "sysdba";
    } elsif ($self->{connect} && ! $self->{username} && ! $self->{password} &&
        $self->{connect} =~ /([\w\-\._]+)/) {
      # --connect bba
      $self->{connect} = $1;
      # maybe this is a os authenticated user
      delete $ENV{TWO_TASK};
      $self->{sid} = $self->{connect};
      if ($^O ne "hpux") {       #hpux && 1.21 only accepts "DBI:Oracle:SID"
        $self->{connect} = "";   #linux 1.20 only accepts "DBI:Oracle:" + ORACLE_SID
      }
      $self->{username} = '/';
      $self->{password} = "";
      $self->{loginstring} = "extauth";
    } elsif ($self->{username} &&
        $self->{username} =~ /^\/@([\w\-\._]+)/) {
      # --user /@ubba1
      $self->{username} = $1;
      $self->{sid} = $self->{connect};
      $self->{loginstring} = "passwordstore";
    } elsif ($self->{connect} && $self->{username} && ! $self->{password} &&
        $self->{username} eq "sysdba") {
      # --connect bba --user sysdba
      $self->{connect} = $1;
      $self->{username} = "/";
      $self->{sid} = $self->{connect};
      $self->{loginstring} = "sysdba";
    } elsif ($self->{connect} && $self->{username} && $self->{password}) {
      # --connect bba --username nagios --password oradbmon
      $self->{sid} = $self->{connect};
      if ($self->{username} eq "sys") {
        delete $ENV{TWO_TASK};
        $self->{loginstring} = "sys";
      } else {
        $self->{loginstring} = "traditional";
      }
    } else {
      $self->{errstr} = "Please specify database, username and password";
      return undef;
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      $ENV{ORACLE_SID} = $self->{sid};
      if (! exists $ENV{ORACLE_HOME}) {
        if ($^O =~ /MSWin/) {
          $self->trace("environment variable ORACLE_HOME is not set");
          foreach my $path (split(';', $ENV{PATH})) {
            $self->trace(sprintf "try to find sqlplus.exe in %s", $path);
            if (-x $path.'/'.'sqlplus.exe') {
              if ($path =~ /[\\\/]bin[\\\/]*$/) {
                $ENV{ORACLE_HOME} = dirname($path);
              } else {
                $ENV{ORACLE_HOME} = $path;
              }
              last;
            }
          }
        } else {
          foreach my $path (split(':', $ENV{PATH})) {
            if (-x $path.'/sqlplus') {
              if ($path =~ /[\/]bin[\/]*$/) {
                $ENV{ORACLE_HOME} = dirname($path);
              } else {
                $ENV{ORACLE_HOME} = $path;
              }
              last;
            }
          }
        }
        if (! exists $ENV{ORACLE_HOME}) {
          $ENV{ORACLE_HOME} |= '';
        } else {
          $self->trace("set ORACLE_HOME = ".$ENV{ORACLE_HOME});
        }
      } else {
        if ($^O =~ /MSWin/) {
          $ENV{PATH} = $ENV{ORACLE_HOME}.';'.$ENV{ORACLE_HOME}.'/'.'bin'.
              (defined $ENV{PATH} ? ";".$ENV{PATH} : "");
          $self->trace("set PATH = ".$ENV{PATH});
        } else {
          $ENV{PATH} = $ENV{ORACLE_HOME}."/bin".
              (defined $ENV{PATH} ? ":".$ENV{PATH} : "");
          $ENV{LD_LIBRARY_PATH} = $ENV{ORACLE_HOME}."/lib".":".$ENV{ORACLE_HOME}.
              (defined $ENV{LD_LIBRARY_PATH} ? ":".$ENV{LD_LIBRARY_PATH} : "");
        }
      }
      # am 30.9.2008 hat perl das /bin/sqlplus in $ENV{ORACLE_HOME}.'/bin/sqlplus' 
      # eiskalt evaluiert und 
      # /u00/app/oracle/product/11.1.0/db_1/u00/app/oracle/product/11.1.0/db_1/bin/sqlplus 
      # draus gemacht. Leider nicht in Mini-Scripts reproduzierbar, sondern nur hier.
      # Das ist der Grund fuer die vielen ' und .
      my $sqlplus = undef;
      my $tnsping = undef;
      $self->trace(sprintf "ORACLE_HOME is now %s", $ENV{ORACLE_HOME});
      my @attempts = ();
      if ($^O =~ /MSWin/) {
        @attempts = qw(bin/sqlplus.exe sqlplus.exe);
      } else {
        @attempts = qw(bin/sqlplus sqlplus);
      }
      foreach my $try (@attempts) {
        $self->trace(sprintf "try to find %s/%s", $ENV{ORACLE_HOME}, $try);
        if (-x $ENV{ORACLE_HOME}.'/'.$try && -f $ENV{ORACLE_HOME}.'/'.$try) {
          $sqlplus = $ENV{ORACLE_HOME}.'/'.$try;
        }
      }
      if (! $sqlplus && -x '/usr/bin/sqlplus') {
        # last hope
        $sqlplus = '/usr/bin/sqlplus';
      }
      if (-x $ENV{ORACLE_HOME}.'/'.'bin'.'/'.'tnsping') {
        $tnsping = $ENV{ORACLE_HOME}.'/'.'bin'.'/'.'tnsping';
      } elsif (-x $ENV{ORACLE_HOME}.'/'.'tnsping') {
        $tnsping = $ENV{ORACLE_HOME}.'/'.'tnsping';
      } elsif (-x $ENV{ORACLE_HOME}.'/'.'tnsping.exe') {
        $tnsping = $ENV{ORACLE_HOME}.'/'.'tnsping.exe';
      } elsif (-x '/usr/bin/tnsping') {
        $tnsping = '/usr/bin/tnsping';
      }
      if (! $sqlplus) {
        die "nosqlplus\n";
      } else {
        $self->trace(sprintf "found %s", $sqlplus);
      }
      if ($self->{mode} =~ /^server::tnsping/) {
        if ($self->{loginstring} eq "traditional") {
          $self->{sqlplus} = sprintf "%s -S \"%s/%s@%s\" < /dev/null",
              $sqlplus,
              $self->{username}, $self->{password}, $self->{sid};
        } elsif ($self->{loginstring} eq "extauth") {
          $self->{sqlplus} = sprintf "%s -S / < /dev/null",
              $sqlplus;
        } elsif ($self->{loginstring} eq "passwordstore") {
          $self->{sqlplus} = sprintf "%s -S /@%s < /dev/null",
              $sqlplus,
              $self->{username};
        } elsif ($self->{loginstring} eq "sysdba") {
          $self->{sqlplus} = sprintf "%s -S / as sysdba < /dev/null",
              $sqlplus;
        } elsif ($self->{loginstring} eq "sys") {
          $self->{sqlplus} = sprintf "%s -S \"%s/%s@%s\" as sysdba < /dev/null",
              $sqlplus,
              $self->{username}, $self->{password}, $self->{sid};
        }
        if ($^O =~ /MSWin/) {
          #$self->{sqlplus} =~ s/ < \/dev\/null//g;
          $self->{sqlplus} =~ s/ < \/dev\/null/ < NUL:/g; # works with powershell
        }
      } else {
        if ($self->{loginstring} eq "traditional") {
          $self->{sqlplus} = sprintf "%s -S \"%s/%s@%s\" < %s > %s",
              $sqlplus,
              $self->{username}, $self->{password}, $self->{sid},
              $self->{sql_commandfile}, $self->{sql_outfile};
        } elsif ($self->{loginstring} eq "extauth") {
          $self->{sqlplus} = sprintf "%s -S / < %s > %s",
              $sqlplus,
              $self->{sql_commandfile}, $self->{sql_outfile};
        } elsif ($self->{loginstring} eq "passwordstore") {
          $self->{sqlplus} = sprintf "%s -S /@%s < %s > %s",
              $sqlplus,
              $self->{username},
              $self->{sql_commandfile}, $self->{sql_outfile};
        } elsif ($self->{loginstring} eq "sysdba") {
          $self->{sqlplus} = sprintf "%s -S / as sysdba < %s > %s",
              $sqlplus,
              $self->{sql_commandfile}, $self->{sql_outfile};
        } elsif ($self->{loginstring} eq "sys") {
          $self->{sqlplus} = sprintf "%s -S \"%s/%s@%s\" as sysdba < %s > %s",
              $sqlplus,
              $self->{username}, $self->{password}, $self->{sid},
              $self->{sql_commandfile}, $self->{sql_outfile};
        }
      }
  
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
  
      if ($self->{mode} =~ /^server::tnsping/) {
        if ($tnsping) {
          my $exit_output = `$tnsping $self->{sid}`;
          if ($?) {
          #  printf STDERR "tnsping exit bumm \n";
          # immer 1 bei misserfolg
          }
          if ($exit_output =~ /^OK \(\d+/m) {
            die "ORA-01017"; # fake a successful connect with wrong password
          } elsif ($exit_output =~ /^(TNS\-\d+)/m) {
            die $1;
          } else {
            die "TNS-03505";
          }
        } else {
          my $exit_output = `$self->{sqlplus}`;
          if ($?) {
            printf STDERR "ping exit bumm \n";
          }
          $exit_output =~ s/\n//g;
          $exit_output =~ s/at $0//g;
          chomp $exit_output;
          die $exit_output;
        }
      } else {
        my $answer = $self->fetchrow_array(
            q{ SELECT 42 FROM dual});
        die $self->{errstr} unless defined $answer and $answer == 42;
      }
      $retval = $self;
    };
    if ($@) {
      $self->{errstr} = $@;
      $self->{errstr} =~ s/at .* line \d+//g;
      chomp $self->{errstr};
      $retval = undef;
    }
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}


sub fetchrow_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my @row = ();
  $sql =~ s/%/%%/g; # sonst mault printf bei "...like '%xy%'"
  $self->trace(sprintf "fetchrow_array: %s", $sql);
  $sql =~ s/%%/%/g;
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }
  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqlplus}`;
  if ($?) {
    my $output = do { local (@ARGV, $/) = $self->{sql_outfile}; my $x = <>; close ARGV; $x } || 'empty';
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs).' '.$exit_output;
  } else {
    # This so-called "best practice" leaves an open filehandle under windows
    # my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; my $x = <>; close ARGV; $x };
    #
    # SELECT count(*) FROM blah
    #                 *
    # ERROR at line 1:
    # ORA-00942: table or view does not exist
    # --> if there is a single * AND ERROR AND ORA then we surely have en error
    if ($output =~ /^\s+\*[ \*]*$/m && 
        $output =~ /^ERROR/m &&
        $output =~/^ORA\-/m) {
      my @oerrs = map {
        /(ORA\-\d+:.*)/ ? $1 : ();
      } split(/\n/, $output);
      $self->{errstr} = join(" ", @oerrs);
    } else {
      if ($output) {
        @row = map { convert($_) }
            map { s/^\s+([\.\d]+)$/$1/g; $_ }         # strip leading space from numbers
            map { s/\s+$//g; $_ }                     # strip trailing space
            split(/\|/, (split(/\n/, $output))[0]);
      }
    }
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper(\@row));
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  unlink $self->{sql_outfile};
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  $sql =~ s/%/%%/g; # sonst mault printf bei "...like '%xy%'"
  $self->trace(sprintf "fetchall_array: %s", $sql);
  $sql =~ s/%%/%/g;
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }

  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqlplus}`;
  if ($?) {
    printf STDERR "fetchrow_array exit bumm %s\n", $exit_output;
    my $output = do { local (@ARGV, $/) = $self->{sql_outfile}; my $x = <>; close ARGV; $x } || 'empty';
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs).' '.$exit_output;
  } else {
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; my $x = <>; close ARGV; $x };
    my @rows = map { [ 
        map { convert($_) } 
        map { s/^\s+([\.\d]+)$/$1/g; $_ }
        map { s/\s+$//g; $_ }
        split /\|/
    ] } grep { ! /^\d+ rows selected/ } 
        grep { ! /^\d+ [Zz]eilen ausgew / }
        grep { ! /^Elapsed: / }
        grep { ! /^\s*$/ } split(/\n/, $output);
    $rows = \@rows;
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper($rows));
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  unlink $self->{sql_outfile};
  return @{$rows};
}

sub func {
  my $self = shift;
  my $function = shift;
  $self->{handle}->func(@_);
}

sub convert {
  my $n = shift;
  # mostly used to convert numbers in scientific notation
  if ($n =~ /^\s*\d+\s*$/) {
    return $n;
  } elsif ($n =~ /^\s*([-+]?)(\d*[\.,]*\d*)[eE]{1}([-+]?)(\d+)\s*$/) {
    my ($vor, $num, $sign, $exp) = ($1, $2, $3, $4);
    $n =~ s/E/e/g;
    $n =~ s/,/\./g;
    $num =~ s/,/\./g;
    my $sig = $sign eq '-' ? "." . ($exp - 1 + length $num) : '';
    my $dec = sprintf "%${sig}f", $n;
    $dec =~ s/\.[0]+$//g;
    return $dec;
  } elsif ($n =~ /^\s*([-+]?)(\d+)[\.,]*(\d*)\s*$/) {
    return $1.$2.".".$3;
  } elsif ($n =~ /^\s*(.*?)\s*$/) {
    return $1;
  } else {
    return $n;
  }
}


sub execute {
  my $self = shift;
  my $sql = shift;
  eval {
    my $sth = $self->{handle}->prepare($sql);
    $sth->execute();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
}

sub DESTROY {
  my $self = shift;
  $self->trace("try to clean up command and result files");
  unlink $self->{sql_commandfile}
      if $self->{sql_commandfile} && -f $self->{sql_commandfile};
  unlink $self->{sql_resultfile}
      if $self->{sql_resultfile} && -f $self->{sql_resultfile};
  unlink $self->{sql_outfile} if
      $self->{sql_outfile} && -f $self->{sql_outfile};
}

sub create_commandfile {
  my $self = shift;
  my $sql = shift;
  open CMDCMD, "> $self->{sql_commandfile}";
  printf CMDCMD "SET HEADING OFF\n";
  printf CMDCMD "SET PAGESIZE 0\n";
  printf CMDCMD "SET LINESIZE 32767\n";
  printf CMDCMD "SET COLSEP '|'\n";
  printf CMDCMD "SET TAB OFF\n";
  printf CMDCMD "SET FEEDBACK OFF\n";
  printf CMDCMD "SET TRIMSPOOL ON\n";
  printf CMDCMD "SET NUMFORMAT 9.999999EEEE\n";
  printf CMDCMD "SPOOL %s\n", $self->{sql_resultfile};
  #  printf CMDCMD "ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,';\n/\n";
  printf CMDCMD "%s\n/\n", $sql;
  printf CMDCMD "SPOOL OFF\n", $sql;
  printf CMDCMD "EXIT\n";
  close CMDCMD;
}

package DBD::Oracle::Server::Connection::Sqlrelay;

use strict;
use Net::Ping;

our @ISA = qw(DBD::Oracle::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      if ($self->{connect} =~ /([\.\w]+):(\d+)/) {
        $self->{host} = $1;
        $self->{port} = $2;
        $self->{socket} = "";
      } elsif ($self->{connect} =~ /([\.\w]+):([\w\/]+)/) {
        $self->{host} = $1;
        $self->{socket} = $2;
        $self->{port} = "";
      }
    }
  } else {
    if (! $self->{connect} || ! $self->{username} || ! $self->{password}) {
      if ($self->{connect} && $self->{connect} =~ /(\w+)\/(\w+)@([\.\w]+):(\d+)/) {
        $self->{username} = $1;
        $self->{password} = $2;
        $self->{host} = $3; 
        $self->{port} = $4;
        $self->{socket} = "";
      } elsif ($self->{connect} && $self->{connect} =~ /(\w+)\/(\w+)@([\.\w]+):([\w\/]+)/) {
        $self->{username} = $1;
        $self->{password} = $2;
        $self->{host} = $3; 
        $self->{socket} = $4;
        $self->{port} = "";
      } else {
        $self->{errstr} = "Please specify database, username and password";
        return undef;
      }
    } else {
      if ($self->{connect} =~ /([\.\w]+):(\d+)/) {
        $self->{host} = $1;
        $self->{port} = $2;
        $self->{socket} = "";
      } elsif ($self->{connect} =~ /([\.\w]+):([\w\/]+)/) {
        $self->{host} = $1;
        $self->{socket} = $2;
        $self->{port} = "";
      } else {
        $self->{errstr} = "Please specify database, username and password";
        return undef;
      }
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      require DBI;
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      if ($self->{handle} = DBI->connect(
          sprintf("DBI:SQLRelay:host=%s;port=%d;socket=%s", $self->{host}, $self->{port}, $self->{socket}),
          $self->{username},
          $self->{password},
          { RaiseError => 1, AutoCommit => $self->{commit}, PrintError => 1 })) {
        $self->{handle}->do(q{
            ALTER SESSION SET NLS_NUMERIC_CHARACTERS=".," });
        $retval = $self;
        if ($self->{mode} =~ /^server::tnsping/ && $self->{handle}->ping()) {
          # database connected. fake a "unknown user"
          $self->{errstr} = "ORA-01017";
        }
      } else {
        $self->{errstr} = DBI::errstr();
      }
    };
    if ($@) {
      $self->{errstr} = $@;
      $self->{errstr} =~ s/at [\w\/\.]+ line \d+.*//g;
      $retval = undef;
    }
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub fetchrow_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my @row = ();
  $self->trace(sprintf "fetchrow_array: %s", $sql);
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    @row = $sth->fetchrow_array();
  };
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
    $self->debug(sprintf "bumm %s", $self->{handle}->errstr());
    # this is useful for mode sql's output
    $self->{errstr} = $self->{handle}->errstr();
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper(\@row));
  if (-f "/tmp/check_oracle_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_oracle_health_simulation/".$self->{mode}; <> };
    @row = split(/\s+/, (split(/\n/, $simulation))[0]);
  }
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  $self->trace(sprintf "fetchall_array: %s", $sql);
  $self->trace(sprintf "args: %s", Data::Dumper::Dumper(\@arguments));
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    $rows = $sth->fetchall_arrayref();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  $self->trace(sprintf "RESULT:\n%s\n",
      Data::Dumper::Dumper($rows));
  if (-f "/tmp/check_oracle_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_oracle_health_simulation/".$self->{mode}; <> };
    @{$rows} = map { [ split(/\s+/, $_) ] } split(/\n/, $simulation);
  }
  return @{$rows};
}

sub func {
  my $self = shift;
  $self->{handle}->func(@_);
}


sub execute {
  my $self = shift;
  my $sql = shift;
  eval {
    my $sth = $self->{handle}->prepare($sql);
    $sth->execute();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
}

sub DESTROY {
  my $self = shift;
  #$self->trace(sprintf "disconnecting DBD %s",
  #    $self->{handle} ? "with handle" : "without handle");
  #$self->{handle}->disconnect() if $self->{handle};
}





package Extraopts;

use strict;
use File::Basename;
use Data::Dumper;

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    file => $params{file},
    commandline => $params{commandline},
    config => {},
    section => 'default_no_section',
  };
  bless $self, $class;
  $self->prepare_file_and_section();
  $self->init();
  return $self;
}

sub prepare_file_and_section {
  my $self = shift;
  if (! defined $self->{file} || ! $self->{file}) {
    # ./check_stuff --extra-opts
    $self->{section} = basename($0);
    $self->{file} = $self->get_default_file();
  } elsif ($self->{file} =~ /^[^@]+$/) {
    # ./check_stuff --extra-opts=special_opts
    $self->{section} = $self->{file};
    $self->{file} = $self->get_default_file();
  } elsif ($self->{file} =~ /^@(.*)/) {
    # ./check_stuff --extra-opts=@/etc/myconfig.ini
    $self->{section} = basename($0);
    $self->{file} = $1;
  } elsif ($self->{file} =~ /^(.*?)@(.*)/) {
    # ./check_stuff --extra-opts=special_opts@/etc/myconfig.ini
    $self->{section} = $1;
    $self->{file} = $2;
  }
}

sub get_default_file {
  my $self = shift;
  foreach my $default (qw(/etc/nagios/plugins.ini
      /usr/local/nagios/etc/plugins.ini
      /usr/local/etc/nagios/plugins.ini
      /etc/opt/nagios/plugins.ini
      /etc/nagios-plugins.ini
      /usr/local/etc/nagios-plugins.ini
      /etc/opt/nagios-plugins.ini)) {
    if (-f $default) {
      return $default;
    }
  }
  return undef;
}

sub init {
  my $self = shift;
  if (! defined $self->{file}) {
    $self->{errors} = sprintf 'no extra-opts file specified and no default file found';
  } elsif (! -f $self->{file}) {
    $self->{errors} = sprintf 'could not open %s', $self->{file};
  } else {
    my $data = do { local (@ARGV, $/) = $self->{file}; <> };
    my $in_section = 'default_no_section';
    foreach my $line (split(/\n/, $data)) {
      if ($line =~ /\[(.*)\]/) {
        $in_section = $1;
      } elsif ($line =~ /(.*?)\s*=\s*(.*)/) {
        $self->{config}->{$in_section}->{$1} = $2;
      }
    }
  }
}

sub is_valid {
  my $self = shift;
  return ! exists $self->{errors};
}

sub overwrite {
  my $self = shift;
  my %commandline = ();
  if (scalar(keys %{$self->{config}->{default_no_section}}) > 0) {
    foreach (keys %{$self->{config}->{default_no_section}}) {
      $commandline{$_} = $self->{config}->{default_no_section}->{$_};
    }
  }
  if (exists $self->{config}->{$self->{section}}) {
    foreach (keys %{$self->{config}->{$self->{section}}}) {
      $commandline{$_} = $self->{config}->{$self->{section}}->{$_};
    }
  }
  foreach (keys %commandline) {
    if (! exists $self->{commandline}->{$_}) {
      $self->{commandline}->{$_} = $commandline{$_};
    }
  }
}



package main;

use strict;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use lib dirname($0);



use vars qw ($PROGNAME $REVISION $CONTACT $TIMEOUT $STATEFILESDIR $needs_restart %commandline);

$PROGNAME = "check_oracle_health";
$REVISION = '$Revision: 1.7.7.3 $';
$CONTACT = 'gerhard.lausser@consol.de';
$TIMEOUT = 60;
$STATEFILESDIR = '/var/tmp/check_oracle_health';
$needs_restart = 0;

my @modes = (
  ['server::tnsping',
      'tnsping', undef,
      'Check the reachability of the server' ],
  ['server::connectiontime',
      'connection-time', undef,
      'Time to connect to the server' ],
  ['server::database::expiredpw',
      'password-expiration', undef,
      'Check the password expiry date for users' ],
  ['server::instance::connectedusers',
      'connected-users', undef,
      'Number of currently connected users' ],
  ['server::instance::sessionusage',
      'session-usage', undef,
      'Percentage of sessions used' ],
  ['server::instance::processusage',
      'process-usage', undef,
      'Percentage of processes used' ],
  ['server::instance::rman::backup::problems',
      'rman-backup-problems', undef,
      'Number of rman backup errors during the last 3 days' ],
  ['server::instance::sga::databuffer::hitratio',
      'sga-data-buffer-hit-ratio', undef,
      'Data Buffer Cache Hit Ratio' ],
  ['server::instance::sga::sharedpool::librarycache::gethitratio',
      'sga-library-cache-gethit-ratio', ['sga-library-cache-hit-ratio'],
      'Library Cache (Get) Hit Ratio' ],
  ['server::instance::sga::sharedpool::librarycache::pinhitratio',
      'sga-library-cache-pinhit-ratio', undef,
      'Library Cache (Pin) Hit Ratio' ],
  ['server::instance::sga::sharedpool::librarycache::reloads',
      'sga-library-cache-reloads', undef,
      'Library Cache Reload (and Invalidation) Rate' ],
  ['server::instance::sga::sharedpool::dictionarycache::hitratio',
      'sga-dictionary-cache-hit-ratio', undef,
      'Dictionary Cache Hit Ratio' ],
  ['server::instance::sga::latch::hitratio',
      'sga-latches-hit-ratio', undef,
      'Latches Hit Ratio' ],
  ['server::instance::sga::sharedpool::reloads',
      'sga-shared-pool-reload-ratio', ['sga-shared-pool-reloads'],
      'Shared Pool Reloads vs. Pins' ],
  ['server::instance::sga::sharedpool::free',
      'sga-shared-pool-free', undef,
      'Shared Pool Free Memory' ],
  ['server::instance::pga::inmemorysortratio',
      'pga-in-memory-sort-ratio', undef,
      'PGA in-memory sort ratio' ],
  ['server::database::invalidobjects',
      'invalid-objects', undef,
      'Number of invalid objects in database' ],
  ['server::database::stalestats',
      'stale-statistics', undef,
      'Find objects with stale optimizer statistics' ],
  ['server::database::blockcorruption',
      'corrupted-blocks', undef,
      'Number of corrupted blocks in database' ],
  ['server::database::tablespace::usage',
      'tablespace-usage', undef,
      'Used space in tablespaces' ],
  ['server::database::tablespace::free',
      'tablespace-free', undef,
      'Free space in tablespaces' ],
  ['server::database::tablespace::remainingfreetime',
      'tablespace-remaining-time', undef,
      'Remaining time until a tablespace is full' ],
  ['server::database::tablespace::fragmentation',
      'tablespace-fragmentation', undef,
      'Free space fragmentation index' ],
  ['server::database::tablespace::iobalance',
      'tablespace-io-balance', undef,
      'balanced io of all datafiles' ],
  ['server::database::tablespace::segment::extendspace',
      'tablespace-can-allocate-next', undef,
      'Segments (of a tablespace) can allocate next extent' ],
  ['server::database::tablespace::datafile::iotraffic',
      'datafile-io-traffic', undef,
      'io operations/per sec of a datafile' ],
  ['server::database::datafilesexisting',
      'datafiles-existing', undef,
      'Percentage of the maximum possible number of datafiles' ],
  ['server::instance::sga::sharedpool::softparse',
      'soft-parse-ratio', undef,
      'Percentage of soft parses' ],
  ['server::instance::sga::redologbuffer::switchinterval',
      'switch-interval', ['redo-switch-interval', 'rac-switch-interval'],
      'Time between redo log file switches' ],
  ['server::instance::sga::redologbuffer::retryratio',
      'retry-ratio', ['redo-retry-ratio'],
      'Redo buffer allocation retries' ],
  ['server::instance::sga::redologbuffer::iotraffic',
      'redo-io-traffic', undef,
      'Redo log io bytes per second' ],
  ['server::instance::sga::rollbacksegments::headercontention',
      'roll-header-contention', undef,
      'Rollback segment header contention' ],
  ['server::instance::sga::rollbacksegments::blockcontention',
      'roll-block-contention', undef,
      'Rollback segment block contention' ],
  ['server::instance::sga::rollbacksegments::hitratio',
      'roll-hit-ratio', undef,
      'Rollback segment hit ratio (gets/waits)' ],
  ['server::instance::sga::rollbacksegments::wraps',
      'roll-wraps', undef,
      'Rollback segment wraps (per sec)' ],
  ['server::instance::sga::rollbacksegments::extends',
      'roll-extends', undef,
      'Rollback segment extends (per sec)' ],
  ['server::instance::sga::rollbacksegments::avgactivesize',
      'roll-avgactivesize', undef,
      'Rollback segment average active size' ],
  ['server::database::tablespace::segment::top10logicalreads',
      'seg-top10-logical-reads', undef,
      'user objects among top 10 logical reads' ],
  ['server::database::tablespace::segment::top10physicalreads',
      'seg-top10-physical-reads', undef,
      'user objects among top 10 physical reads' ],
  ['server::database::tablespace::segment::top10bufferbusywaits',
      'seg-top10-buffer-busy-waits', undef,
      'user objects among top 10 buffer busy waits' ],
  ['server::database::tablespace::segment::top10rowlockwaits',
      'seg-top10-row-lock-waits', undef,
      'user objects among top 10 row lock waits' ],
  ['server::instance::event::waits',
      'event-waits', undef,
      'processes wait events' ],
  ['server::instance::event::waiting',
      'event-waiting', undef,
      'time spent by processes waiting for an event' ],
  ['server::instance::enqueue::contention',
      'enqueue-contention', undef,
      'percentage of enqueue requests which must wait' ],
  ['server::instance::enqueue::waiting',
      'enqueue-waiting', undef,
      'percentage of time spent waiting for the enqueue' ],
  ['server::instance::sga::latch::contention',
      'latch-contention', undef,
      'percentage of latch get requests which must wait' ],
  ['server::instance::sga::latch::waiting',
      'latch-waiting', undef,
      'percentage of time a latch spends sleeping' ],
  ['server::instance::sysstat::rate',
      'sysstat', undef,
      'change of sysstat values over time' ],
  ['server::database::flash_recovery_area::usage',
      'flash-recovery-area-usage', undef,
      'Used space in flash recovery area' ],
  ['server::database::flash_recovery_area::free',
      'flash-recovery-area-free', undef,
      'Free space in flash recovery area' ],
  ['server::sql',
      'sql', undef,
      'any sql command returning a single number' ],
  ['server::sqlruntime',
      'sql-runtime', undef,
      'the time an sql command needs to run' ],
  ['server::database::tablespace::listtablespaces',
      'list-tablespaces', undef,
      'convenience function which lists all tablespaces' ],
  ['server::database::tablespace::datafile::listdatafiles',
      'list-datafiles', undef,
      'convenience function which lists all datafiles' ],
  ['server::instance::enqueue::listenqueues',
      'list-enqueues', undef,
      'convenience function which lists all enqueues' ],
  ['server::instance::sga::latch::listlatches',
      'list-latches', undef,
      'convenience function which lists all latches' ],
  ['server::instance::event::listevents',
      'list-events', undef,
      'convenience function which lists all events' ],
  ['server::instance::event::listeventsbg',
      'list-background-events', undef,
      'convenience function which lists all background events' ],
  ['server::instance::sysstat::listsysstats',
      'list-sysstats', undef,
      'convenience function which lists all statistics from v$sysstat' ],
);

sub print_usage () {
  print <<EOUS;
  Usage:
    $PROGNAME [-v] [-t <timeout>] --connect=<connect string>
        --username=<username> --password=<password> --mode=<mode>
        --tablespace=<tablespace>
    $PROGNAME [-h | --help]
    $PROGNAME [-V | --version]

  Options:
    --connect
       the connect string
    --username
       the oracle user
    --password
       the oracle user's password
    --warning
       the warning range
    --critical
       the critical range
    --mode
       the mode of the plugin. select one of the following keywords:
EOUS
  my $longest = length ((reverse sort {length $a <=> length $b} map { $_->[1] } @modes)[0]);
  my $format = "       %-".
  (length ((reverse sort {length $a <=> length $b} map { $_->[1] } @modes)[0])).
  "s\t(%s)\n";
  foreach (@modes) {
    printf $format, $_->[1], $_->[3];
  }
  printf "\n";
  print <<EOUS;
    --name
       the name of the tablespace, datafile, wait event, 
       latch, enqueue, or sql statement depending on the mode.
    --name2
       if name is a sql statement, this statement would appear in
       the output and the performance data. This can be ugly, so 
       name2 can be used to appear instead.
    --regexp
       if this parameter is used, name will be interpreted as a 
       regular expression.
    --units
       one of %, KB, MB, GB. This is used for a better output of mode=sql
       and for specifying thresholds for mode=tablespace-free
    --ident
       outputs instance and database names
    --commit
       turns on autocommit for the dbd::oracle module

  Tablespace-related modes check all tablespaces in one run by default.
  If only a single tablespace should be checked, use the --name parameter.
  The same applies to datafile-related modes.
  If an additional --regexp is added, --name's argument will be interpreted
  as a regular expression.
  The parameter --mitigation lets you classify the severity of an offline
  tablespace. 

  tablespace-remaining-time will take historical data into account. The number
  of days in the past can be given with the --lookback parameter. (Default: 30)
  
  In mode sql you can url-encode the statement so you will not have to mess
  around with special characters in your Nagios service definitions.
  Instead of 
  --name="select count(*) from v\$session where status = 'ACTIVE'"
  you can say 
  --name=select%20count%28%2A%29%20from%20v%24session%20where%20status%20%3D%20%27ACTIVE%27
  For your convenience you can call check_oracle_health with the --encode
  option and it will encode the standard input.

EOUS
#
# --basis
#  one of rate, delta, value
  
}

sub print_help () {
  print "Copyright (c) 2008 Gerhard Lausser\n\n";
  print "\n";
  print "  Check various parameters of Oracle databases \n";
  print "\n";
  print_usage();
  support();
}


sub print_revision ($$) {
  my $commandName = shift;
  my $pluginRevision = shift;
  $pluginRevision =~ s/^\$Revision: //;
  $pluginRevision =~ s/ \$\s*$//;
  print "$commandName ($pluginRevision)\n";
  print "This nagios plugin comes with ABSOLUTELY NO WARRANTY. You may redistribute\ncopies of this plugin under the terms of the GNU General Public License.\n";
}

sub support () {
  my $support='Send email to gerhard.lausser@consol.de if you have questions\nregarding use of this software. \nPlease include version information with all correspondence (when possible,\nuse output from the --version option of the plugin itself).\n';
  $support =~ s/@/\@/g;
  $support =~ s/\\n/\n/g;
  print $support;
}

sub contact_author ($$) {
  my $item = shift;
  my $strangepattern = shift;
  if ($commandline{verbose}) {
    printf STDERR
        "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n".
        "You found a line which is not recognized by %s\n".
        "This means, certain components of your system cannot be checked.\n".
        "Please contact the author %s and\nsend him the following output:\n\n".
        "%s /%s/\n\nThank you!\n".
        "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n",
            $PROGNAME, $CONTACT, $item, $strangepattern;
  }
}

%commandline = ();
my @params = (
    "timeout|t=i",
    "version|V",
    "help|h",
    "verbose|v",
    "debug|d",
    "connect|c=s",
    "username|u=s",
    "password|p=s",
    "mode|m=s",
    "tablespace=s",
    "datafile=s",
    "waitevent=s",
    "offlineok",
    "mitigation=s",
    "notemp",
    "noreadonly",
    "name=s",
    "name2=s",
    "regexp",
    "perfdata",
    "warning=s",
    "critical=s",
    "dbthresholds:s",
    "absolute|a",
    "basis",
    "lookback|l=i",
    "environment|e=s%",
    "calcmeth=s",
    "method=s",
    "runas|r=s",
    "scream",
    "shell",
    "eyecandy",
    "encode",
    "units=s",
    "ident",
    "3",
    "statefilesdir=s",
    "with-mymodules-dyn-dir=s",
    "report=s",
    "commit",
    "extra-opts:s");

if (! GetOptions(\%commandline, @params)) {
  print_help();
  exit $ERRORS{UNKNOWN};
}

if (exists $commandline{'extra-opts'}) {
  # read the extra file and overwrite other parameters
  my $extras = Extraopts->new(file => $commandline{'extra-opts'}, commandline => \%commandline);
  if (! $extras->is_valid()) {
    printf "extra-opts are not valid: %s\n", $extras->{errors};
    exit $ERRORS{UNKNOWN};
  } else {
    $extras->overwrite();
  }
}

if (exists $commandline{version}) {
  print_revision($PROGNAME, $REVISION);
  exit $ERRORS{OK};
}

if (exists $commandline{help}) {
  print_help();
  exit $ERRORS{OK};
} elsif (! exists $commandline{mode}) {
  printf "Please select a mode\n";
  print_help();
  exit $ERRORS{OK};
}

if ($commandline{mode} eq "encode") {
  my $input = <>;
  chomp $input;
  $input =~ s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
  printf "%s\n", $input;
  exit $ERRORS{OK};
}

if (exists $commandline{3}) {
  $ENV{NRPE_MULTILINESUPPORT} = 1;
}

if (exists $commandline{timeout}) {
  $TIMEOUT = $commandline{timeout};
}

if (exists $commandline{verbose}) {
  $DBD::Oracle::Server::verbose = exists $commandline{verbose};
}

if (exists $commandline{scream}) {
#  $DBD::Oracle::Server::hysterical = exists $commandline{scream};
}

if (exists $commandline{method}) {
  # dbi, snmp or sqlplus
} else {
  $commandline{method} = "dbi";
}

if (exists $commandline{report}) {
  # short, long, html
} else {
  $commandline{report} = "long";
}

if (exists $commandline{commit}) {
  $commandline{commit} = 1;
} else {
  $commandline{commit} = 0;
}

if (exists $commandline{calcmeth}) {
  if ($commandline{calcmeth} ne "classic" && $commandline{calcmeth} ne "sap" && $commandline{calcmeth} ne "classic") {
    printf "Parameter calcmeth must be classic (which is the default), sap or java\n";
    print_help();
    exit $ERRORS{OK};
  }
} else {
  $commandline{calcmeth} = "classic";
}

if (exists $commandline{'with-mymodules-dyn-dir'}) {
  $DBD::Oracle::Server::my_modules_dyn_dir = $commandline{'with-mymodules-dyn-dir'};
} else {
  $DBD::Oracle::Server::my_modules_dyn_dir = '/usr/local/nagios/libexec';
}

if (exists $commandline{environment}) {
  # if the desired environment variable values are different from
  # the environment of this running script, then a restart is necessary.
  # because setting $ENV does _not_ change the environment of the running script.
  foreach (keys %{$commandline{environment}}) {
    if ((! $ENV{$_}) || ($ENV{$_} ne $commandline{environment}->{$_})) {
      $needs_restart = 1;
      $ENV{$_} = $commandline{environment}->{$_};
      printf STDERR "new %s=%s forces restart\n", $_, $ENV{$_} 
          if $DBD::Oracle::Server::verbose;
    }
  }
  # e.g. called with --runas dbnagio. shlib_path environment variable is stripped
  # during the sudo.
  # so the perl interpreter starts without a shlib_path. but --runas cares for
  # a --environment shlib_path=...
  # so setting the environment variable in the code above and restarting the 
  # perl interpreter will help it find shared libs
}

if (exists $commandline{runas}) {
  # remove the runas parameter
  # exec sudo $0 ... the remaining parameters
  $needs_restart = 1;
  # if the calling script has a path for shared libs and there is no --environment
  # parameter then the called script surely needs the variable too.
  foreach my $important_env (qw(LD_LIBRARY_PATH SHLIB_PATH 
      ORACLE_HOME TNS_ADMIN ORA_NLS ORA_NLS33 ORA_NLS10)) {
    if ($ENV{$important_env} && ! scalar(grep { /^$important_env=/ } 
        keys %{$commandline{environment}})) {
      $commandline{environment}->{$important_env} = $ENV{$important_env};
      printf STDERR "add important --environment %s=%s\n", 
          $important_env, $ENV{$important_env} if $DBD::Oracle::Server::verbose;
    }
  }
}

if ($needs_restart) {
  my @newargv = ();
  my $runas = undef;
  if (exists $commandline{runas}) {
    $runas = $commandline{runas};
    delete $commandline{runas};
  }
  foreach my $option (keys %commandline) {
    if (grep { /^$option/ && /=/ } @params) {
      if (ref ($commandline{$option}) eq "HASH") {
        foreach (keys %{$commandline{$option}}) {
          push(@newargv, sprintf "--%s", $option);
          push(@newargv, sprintf "%s=%s", $_, $commandline{$option}->{$_});
        }
      } else {
        push(@newargv, sprintf "--%s", $option);
        push(@newargv, sprintf "%s", $commandline{$option});
      }
    } elsif (grep { /^$option/ && /:/ } @params) {
      if ($commandline{$option}) {
        push(@newargv, sprintf "--%s", $option);
        push(@newargv, sprintf "%s", $commandline{$option});
      } else {
        push(@newargv, sprintf "--%s", $option);
      }
    } else {
      push(@newargv, sprintf "--%s", $option);
    }
  }
  if ($runas && ($> == 0)) {
    # this was not my idea. some people connect as root to their nagios clients.
    exec "su", "-c", sprintf("%s %s", $0, join(" ", @newargv)), "-", $runas;
  } elsif ($runas) {
    exec "sudo", "-S", "-u", $runas, $0, @newargv;
  } else {
    exec $0, @newargv;  
    # this makes sure that even a SHLIB or LD_LIBRARY_PATH are set correctly
    # when the perl interpreter starts. Setting them during runtime does not
    # help loading e.g. libclntsh.so
  }
  exit;
}

if (exists $commandline{shell}) {
  # forget what you see here.
  system("/bin/sh");
}

if (! exists $commandline{statefilesdir}) {
  if (exists $ENV{OMD_ROOT}) {
    $commandline{statefilesdir} = $ENV{OMD_ROOT}."/var/tmp/check_oracle_health";
  } else {
    $commandline{statefilesdir} = $STATEFILESDIR;
  }
}

if (exists $commandline{name}) {
  if ($^O =~ /MSWin/ && $commandline{name} =~ /^'(.*)'$/) {
    # putting arguments in single ticks under Windows CMD leaves the ' intact
    # we remove them
    $commandline{name} = $1;
  }
  # objects can be encoded like an url
  # with s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
  if (($commandline{mode} ne "sql") || 
      (($commandline{mode} eq "sql") &&
       ($commandline{name} =~ /select%20/i))) { # protect ... like '%cac%' ... from decoding
    $commandline{name} =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg;
  }
  if ($commandline{name} =~ /^0$/) {
    # without this, $params{selectname} would be treated like undef
    $commandline{name} = "00";
  } 
}

$SIG{'ALRM'} = sub {
  printf "UNKNOWN - %s timed out after %d seconds\n", $PROGNAME, $TIMEOUT;
  exit $ERRORS{UNKNOWN};
};
alarm($TIMEOUT);

my $nagios_level = $ERRORS{UNKNOWN};
my $nagios_message = "";
my $perfdata = "";
my $racmode = 0;
if ($commandline{mode} =~ /^rac-([^\-.]+)/) {
  $racmode = 1;
  $commandline{mode} =~ s/^rac\-//g;
}
if ($commandline{mode} =~ /^my-([^\-.]+)/) {
  my $param = $commandline{mode};
  $param =~ s/\-/::/g;
  push(@modes, [$param, $commandline{mode}, undef, 'my extension']);
} elsif ((! grep { $commandline{mode} eq $_ } map { $_->[1] } @modes) &&
    (! grep { $commandline{mode} eq $_ } map { defined $_->[2] ? @{$_->[2]} : () } @modes)) {
  printf "UNKNOWN - mode %s\n", $commandline{mode};
  print_usage();
  exit 3;
}

my %params = (
    timeout => $TIMEOUT,
    mode => (
        map { $_->[0] } 
        grep {
           ($commandline{mode} eq $_->[1]) || 
           ( defined $_->[2] && grep { $commandline{mode} eq $_ } @{$_->[2]}) 
        } @modes
    )[0],
    cmdlinemode => $commandline{mode},
    racmode => $racmode,
    method => $commandline{method} ||
        $ENV{NAGIOS__SERVICEORACLE_METH} ||
        $ENV{NAGIOS__HOSTORACLE_METH} || 'dbi',
    connect => $commandline{connect}  || 
        $ENV{NAGIOS__SERVICEORACLE_SID} ||
        $ENV{NAGIOS__HOSTORACLE_SID} ||
        $ENV{ORACLE_SID},
    username => $commandline{username} || 
        $ENV{NAGIOS__SERVICEORACLE_USER} ||
        $ENV{NAGIOS__HOSTORACLE_USER},
    password => $commandline{password} || 
        $ENV{NAGIOS__SERVICEORACLE_PASS} ||
        $ENV{NAGIOS__HOSTORACLE_PASS},
    warningrange => $commandline{warning},
    criticalrange => $commandline{critical},
    dbthresholds => $commandline{dbthresholds},
    absolute => $commandline{absolute},
    lookback => $commandline{lookback},
    tablespace => $commandline{tablespace},
    datafile => $commandline{datafile},
    basis => $commandline{basis},
    offlineok => $commandline{offlineok},
    mitigation => $commandline{mitigation},
    notemp => $commandline{notemp},
    noreadonly => $commandline{noreadonly},
    selectname => $commandline{name} || $commandline{tablespace} || $commandline{datafile},
    regexp => $commandline{regexp},
    name => $commandline{name},
    name2 => $commandline{name2} || $commandline{name},
    units => $commandline{units},
    eyecandy => $commandline{eyecandy},
    statefilesdir => $commandline{statefilesdir},
    ident => $commandline{ident},
    verbose => $commandline{verbose},
    report => $commandline{report},
    commit => $commandline{commit},
    calcmeth => $commandline{calcmeth},
);

my $server = undef;

$server = DBD::Oracle::Server->new(%params);
$server->nagios(%params);
$server->calculate_result();
$nagios_message = $server->{nagios_message};
$nagios_level = $server->{nagios_level};
$perfdata = $server->{perfdata};

printf "%s - %s", $ERRORCODES{$nagios_level}, $nagios_message;
printf " | %s", $perfdata if $perfdata;
printf "\n";
exit $nagios_level;
