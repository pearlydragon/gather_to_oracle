#!/usr/bin/perl

use strict;
use Path::Class;
use DBI;
use DBD::Oracle;
use Switch;

if ( $ARGV[0] )
{
    my $var0=0;
} else
{
  print "Need parameter!!!";
  exit 0;
}

my $nomer = $ARGV[0];
my $nomer1 = $ARGV[0]-0;
my $stopfile="/somefolder/to_ora/stop/ora_m$nomer"; #need for stop gathering.
my $str_r;
my $r_path='/folder for gather/';
my $path=$r_path . $nomer;
my $rpath=dir("$r_path" . "$nomer");
my $dirw = dir("/somedisk/");
my $diru = dir("/somedisk/");
my $filew = $dirw->file("u_o_" . $nomer1 . "_pl.test");
my $fileu = $diru->file("w_o_" . $nomer1 . "_pl.test");
my $db;
my $fail=0;
my $tolog='';

#Проверим, запущено ли.
if ( -s "/somefolder/d/to_ora_$nomer.pid" )
{
    my $var0=0;
} else
{
    `touch "/somefolder/d/to_ora_$nomer.pid"`;
    `echo "9999999999" > "/somefolder/d/to_ora_$nomer.pid"`;
}
my $pidfile=file("/somefolder/d/to_ora_$nomer.pid");
my $content = $pidfile->slurp();
my $file_handle = $pidfile->openr();
my $line = $file_handle->getline();
close $file_handle;
if ( `pidof -x to_ora.pl | grep -w '$line' | wc -l` == "1" )
{
    `date +%d/%m/%y-%T >> log/while.log`;
    `echo "$nomer Уже запущено. Fail!" >> log/while.log`;
    exit 0;
} else
{
    `date +%d/%m/%y-%T >> log/while.log`;
    `echo "$nomer Всё нормально. All right. Run." >> log/while.log`;
    `echo $$ > "/somefolder/d/to_ora_$nomer.pid"`;
}
#----------------------

r_connect();

while ( ! -e $stopfile )
{
  system ("date +%d/%m/%y-%T");
  print "dbstate: ", $db->state, "\n";
  if ($db->state ne "")
  {
    	sleep(300);
    	r_connect();
      if ($db->state ne "")
      {
        next;
      }
  }
  $str_r='';
  get_R_data();
  
  sleep(15);
}
$db->disconnect();
#---------------------------------------------------------------------------SUBS
sub print_to_daylylog{
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
$mon=$mon+1;
$year=$year+1990;
my $dlog_file='log/'. $mday .'_'. $mon .'_'. $year.'_R_m'.$nomer.'.log'; 
open (my $dlog, '>>', $dlog_file);
print $dlog "$tolog";
close $dlog;
}

sub get_R_data{
  open (my $fhw, '>', $filew);
  open (my $fhu, '>', $fileu);
  print $fhw ':)';
  print $fhu ':)';
  close $fhw;
  close $fhu;
  if ( -s $filew && -s $fileu )
  {
    unlink $filew;
    unlink $fileu;
  } else
  {
    print "Disks not found!!!";
    return;
  }
  opendir(my $dh, "$path");
  my @files = grep { -f "$path/$_" && /^R/ } readdir($dh);
  closedir($dh);
  my $files_count=0;
  if (@files)
  {
    foreach my $file (@files)
    {
      if ( -z $file)
      {
        print 'File have size=0! (((',"\n";
        unlink "$path/$file";
        return;
      }
      $tolog="$file\n";
      print_to_daylylog();
      my $filer = $rpath->file("$file");
      my $line_number=0;
      my $content = $filer->slurp();
      my $file_handle = $filer->openr();
      while( my $line = $file_handle->getline() )
      {
      	my $temp_line=$line;
      	$temp_line =~ s/([\r\n])//g;
        $line_number++;
      	$str_r .= "db_function('".$line_number."','".$temp_line."');\n";
        $tolog="db_function('".$line_number."','".$temp_line."');\n";
        print_to_daylylog();
      }
      close $file_handle;
      if ($str_r ne "")
      {
	my $st = $db->prepare('begin'."\n$str_r\n".'end;') or print "Failed execute content of $file!! ";
	check_op();
	if ($fail == 1){
	    $db->rollback;
	    $str_r='';
	    #$st->finish;
	    next;
	} else
	{
	    $st->execute() or print "Failed execute content of $file!!  ";
	    check_op();
	    if ($fail == 1){
		$db->rollback;
		$str_r='';
		$st->finish;
		next;
	    } else
	    {
		$db->commit;
		$st->finish;
	    }
	}
      }
      if ($fail == 1){
	  print "file $file not loaded";
	  next;
      } else
      {
	  unlink "$path/$file";
      }
      $str_r='';
#       $files_count++;
    }
  }
}
##############################
sub r_connect{
  $db=DBI->connect("dbi:Oracle:$nomer", "$nomer1", "$nomer1", {RaiseError => 0}) or print "Connection failed!! ";
  $db->{ora_module_name} = 'to_ora_' . $nomer;
  $db->{ora_client_info} = 'to_ora_' . $nomer;
  $db->{ora_action} = 'Gather to DB';
  $db->{AutoCommit} = 0;
  check_op();
}
##############################
sub check_op{
  switch ($db->errstr){
    case /ORA-03135/ { print "Соединение потеряно\n";
      $fail=1;
      sleep(300);
      r_connect();
    }
    case /ORA-03114/ { print "Нет связи\n";
      $fail=1;
      sleep(300);
      r_connect();
    }
    case /ORA-20000/ { print "Ошибка загрузки\n";
      $fail=1;
      #$db->rollback;
    }
    case /ORA-01756/{ print "Ошибка загрузки\n";
      $fail=1;
      #$db->rollback;
    }
    else {$fail=0;}
  }
  return;
}
#---------------------------------------------------------------------------SUBS
unlink $stopfile;
exit;
