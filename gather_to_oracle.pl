#!/usr/bin/perl
################################################################################
#Скрипт,загружающий в БД данные из R-чеков, сохраняя один коннект на протяжении
#всей работы.
################################################################################

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

my $version = "1.18.3";

my $nomer = $ARGV[0];
my $nomer1 = $ARGV[0]-0;
if ($nomer == "00"){$nomer1 = $ARGV[0];}
my $stopfile="to_ora/stop/to_ora_m$nomer";
my $str_r;
my $r_path='';
my $path='';
my $rpath='';
if ($nomer != 00){
  $r_path='Infolder_000';
  $path=$r_path . $nomer;
  $rpath=dir("$r_path" . "$nomer");
}
else {
  $r_path='In_folder';
  $path=$r_path;
  $rpath=dir("$r_path");
}

my $dirw = dir("w/");
my $diru = dir("u/");
my $filew = $dirw->file("u_o_" . $nomer1 . "_pl.test");
my $fileu = $diru->file("w_o_" . $nomer1 . "_pl.test");
my $db;
my $fail=0;
my $tolog='';
my $path_d="to_ora";
my $line;

my ($nal, $s_nal, $beznal, $s_beznal, $summ, $pay, $diss, $card) = 0;

#Проверим, запущено ли.
if ( -s "$path_d/to_ora_m$nomer.pid" )
{
    my $var0=0;
} else
{
    `touch "$path_d/to_ora_m$nomer.pid"`;
    `echo "9999999999" > "$path_d/to_ora_m$nomer.pid"`;
}
my $pidfile=file("$path_d/to_ora_m$nomer.pid");
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
    `echo $$ > "$path_d/to_ora_m$nomer.pid"`;
}
#----------------------
print "Hello! What a nice day for work!)\n";
r_connect();

while ( ! -e $stopfile )
{
  system ("date +%d/%m/%y-%T");
  print "dbstate: ", $db->state, "\n";
  if ($db->state ne "")
  {
      $db->disconnect();
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
print "Good bay!\n";
$db->disconnect();
#---------------------------------------------------------------------------SUBS
sub print_to_daylylog{
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    $mon=$mon+1;
    $year=$year+1900;
    my $dlog_file='to_ora/R/'.'R_m'.$nomer.'.log';
    open (my $dlog, '>>', $dlog_file);
    print $dlog "$tolog";
    close $dlog;
}
##############################
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
  } else {
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
      if ( -z $file and -e $file)
      {
        print 'File have size=0! (((',"\n";
        unlink "$path/$file";
        return;
      }

      my $temp_line;
      my $data1 = `tail -n 1 $path/$file`;
      ($s_nal, $s_beznal, $nal, $beznal, $summ) = 0;
      $card = $data1;
      $card =~ m/LOYMCARDD=(\d*;)/;
      $card = $1;

      my $str_count = `cat $path/$file | wc -l`;
      my $strochka = 1;
      print "$str_count, $strochka\n";
      while ($str_count > $strochka){
          my $data = `tail -n $strochka $path/$file | head -n 1`;
          $_ = $data;
          if ( /^#/ ){
              ($nal,$beznal,$summ) = split(/;/,$data);
              $s_nal = $summ;
              $s_beznal = 0;
              last;
          }
          if (! /^\d/ ){
            print "$strochka/$str_count\n";
            ($nal,$beznal,$summ) = split(/;/,$data);
            $s_nal += "$nal";
            $s_beznal += "$beznal";
          } else { last; }
          $strochka++;
      }

      $tolog="$file\n";
      print_to_daylylog();
      my $filer = $rpath->file("$file");
      my $line_number=0;
      my $content = $filer->slurp();
      my $file_handle = $filer->openr();
      while( $line = $file_handle->getline() ){
        	$_ = $line;
        	next if ! /^\d/ ;
        	my $pmix = $line;
        	$pmix =~ m/PMIX=(\d*;)/;

        	if ($1 and $1 ne ""){
        	    $pmix = "$1";
        	}else{
        	    $pmix = ";";
        	}

        	my $temp_line = $line;
        	$temp_line =~ m/(;)/;
        	my $pmixt = $line;

        	$pmixt =~ m/PMIXT=(\w*;)/;
        	if ($1 and $1 ne ""){
        	    $pmixt = "$1";
        	}else{
        	    $pmixt = ";";
        	}

        	$temp_line = $line;
        	$temp_line =~ m/(;)/;
        	my $pcoupon = $line;

        	$pcoupon =~ m/PCOUPON=(\w*;)/;
        	if ($1 and $1 ne ""){
        	    $pcoupon = "$1";
        	}else{
        	    $pcoupon = ";";
        	}

        	$temp_line = $line;
        	$temp_line =~ m/(;)/;
        	my $psetnum = $line;

        	$psetnum =~ m/PSETNUM=(\d*;)/;
        	if ($1 and $1 ne ""){
        	    $psetnum = "$1";
        	}else{
        	    $psetnum = ";";
        	}

        	$temp_line = $line;
        	$temp_line =~ m/(;)/;
        	my $psetcode = $line;

        	$psetcode =~ m/PSETCODE=(\d*;)/;
        	if ($1 and $1 ne ""){
        	    $psetcode = "$1";
        	}else{
        	    $psetcode = ";";
          }
        	$temp_line = $line;
        	$temp_line =~ m/(;)/;
        	my $am = $line;
        	$am =~ m/AM=(\w*;)/;
        	if ($1 and $1 ne ""){
        	    $am = "$1";
        	}else{
        	    $am = ";";
        	}
          $temp_line = $line;
          $temp_line =~ m/(;)/;
          $diss = $line;
          $diss =~ m/LOYMDISC=(\d*.\d*;)/;
          $diss = $1;
          $temp_line = $line;
          $temp_line =~ m/(;)/;
          $pay = $line;
          $pay =~ m/LOYMPAYB=(\d*.\d*;)/;
          $pay = $1;
        	print substr($line, 0, 128), "$am$pmix$pmixt$pcoupon$psetnum$psetcode$s_nal;$s_beznal;$summ;$pay$diss$card\n";

        	my $temp_line=substr($line, 0, 128);
        	$temp_line =~ s/([\r\n])//g;
          $temp_line =~ s/([ ])//g;
          $line_number++;
          my ($f1, $f2, $f3, $f4, $f5, $f6, $f7, $f8, $f9, $f9, $f10, $f11, $f12) = split(/;/,$temp_line);

          if ($diss == 0){
              $diss = ";";
          }
        	if ($line_number == 1){
              if ($f6 == "02"){
                  $summ=$summ*-1;
                  #$s_nal=$s_nal*-1;
                  $s_beznal=$s_beznal*-1;
              }
              $str_r .= "checkon.check_put('".$line_number."','".$temp_line.$am.$pmix.$pmixt.$pcoupon.$psetnum.$psetcode.$s_nal.";".$s_beznal.";".$summ.";".$pay.$diss.$card."');\n";
              $tolog="checkon.check_put('".$line_number."','".$temp_line.$am.$pmix.$pmixt.$pcoupon.$psetnum.$psetcode.$s_nal.";".$s_beznal.";".$summ.";".$pay.$diss.$card."');\n";
          }
          else{
              $str_r .= "checkon.check_put('".$line_number."','".$temp_line.$am.$pmix.$pmixt.$pcoupon.$psetnum.$psetcode.";;;".$pay.$diss.";"."');\n";
              $tolog="checkon.check_put('".$line_number."','".$temp_line.$am.$pmix.$pmixt.$pcoupon.$psetnum.$psetcode.";;;".$pay.$diss.";"."');\n";
          }
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
          	} else{
        	    $st->execute() or print "Failed execute content of $file!!  ";
        	    check_op();
        	    if ($fail == 1){
          		    $db->rollback;
          		    $str_r='';
          		    $st->finish;
          		    next;
          	  } else{
          		    $db->commit;
          		    $st->finish;
          	  }
          	}
        }
        if ($fail == 1){
  	         print "file $file not loaded";
  	         next;
        } else {
  	       unlink "$path/$file";
        }
        $str_r='';
  #       $files_count++;

    }
  }
}
##############################
sub r_connect{
  $db=DBI->connect("dbi:Oracle:ETK$nomer", "ETK$nomer1", "ETK$nomer1", {RaiseError => 0}) or print "Connection failed!! ";
  $db->{ora_module_name} = 'to_ora_' . $nomer;
  $db->{ora_client_info} = 'to_ora_' . $nomer." v. ".$version;
  $db->{ora_action} = 'Gather R to DB';
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
##############################
#---------------------------------------------------------------------------SUBS
unlink $stopfile;
exit;

