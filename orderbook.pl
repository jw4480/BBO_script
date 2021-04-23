use strict;
use warnings;
use POSIX;
use Getopt::Long;
use Data::Dumper;
use Term::ANSIColor;
#TODO
#Report error if Market order volume is bigger than total trade volume
#Count total of nonexistent order cancellations

#init argvs

my $ticker_ref;
my $date_ref;
my $tradefile;
my $orderfile;
my $debug;
my $path;

GetOptions (
            'ticker:s' => \$ticker_ref,
            'date=i' => \$date_ref,
            'tradefile:s' => \$tradefile,
            'orderfile:s' => \$orderfile,
            'path:s' => \$path,
            'debug' => \$debug,
          );
if($debug){
    print "Trade file path : $tradefile\n";
    print "Order file path : $orderfile\n";
}

#init vars

my $nonexistentcancel = 0;
my $firstsell = 1;
my $firstbuy = 1;

my ($ticker, $time, $id, $price, $volume, $otype, $BSflag) = ();
my ($tradeTicker, $tradeTime, $nID, $tradePrice, $tradeVolume, $turnover, $tradeBSflag, $orderKind, $functionCode, $askOrder, $bidOrder) = ();
#!price volume id
my $OPEN_ORDERS = {};
my @OPEN_SELL_ORDERS = ();
my @OPEN_BUY_ORDERS = ();


#Array of order IDs
my @BBO_UPDATED_ORDERS = ();


sub findOrder{
    my $id = shift;
    foreach my $flag (keys %{$OPEN_ORDERS}){
        foreach my $price (keys %{$OPEN_ORDERS->{$flag}}){
            foreach my $refid (keys %{$OPEN_ORDERS->{$flag}->{$price}}){
                if($refid == $id){
                    return $OPEN_ORDERS->{$flag}->{$price}->{$refid};
                }
            }
        }
    }
}
sub convertTime{
    my $time = shift;
    my $hour = $time;
    $hour = substr($hour, 0, -3);
    my $ms = substr($time, -3);
    if(length($time) == 8){
	return "0$hour.$ms";
    }else{
	return "$hour.$ms";
    }
}
sub convertPrice{
	my $price_ref = shift;
	my $new_price = sprintf('%.2f',$price_ref / 10000);
	return $new_price

}
sub getTotal{
    my $ref_flag = shift;
    my $ref_price = shift;
    my $total = 0;
    foreach my $ref_id (keys %{$OPEN_ORDERS->{$ref_flag}->{$ref_price}}){
        my @line = split(' ', $OPEN_ORDERS->{$ref_flag}->{$ref_price}->{$ref_id});
        $total = $total + $line[1];
    }
    return $total;
}
sub getBBO{
    my @keys;
    #  print Dumper \%{$OPEN_ORDERS->{"B"}};
    #  print "\n";
    foreach my $ref_price (keys %{$OPEN_ORDERS->{"B"}}){
        if(%{$OPEN_ORDERS->{"B"}->{$ref_price}}){
            # print $ref_price;
            # print "\n";
            push @keys, $ref_price;
        }
    }
    if(!(@keys)){
        return 0;
    }
    @keys = reverse sort { $a <=> $b } @keys;
    
    foreach my $k (@keys){
        return $k;
        # print $k;
        # print "\n";
    }
}

sub getBSO{
    my @keys;
    foreach my $ref_price (keys %{$OPEN_ORDERS->{"S"}}){
        if(%{$OPEN_ORDERS->{"S"}->{$ref_price}}){
            # print $ref_price;
            # print "\n";
            push @keys, $ref_price;
        }
    }
    if(!(@keys)){
        return 0;
    }
    @keys = sort {$a <=> $b} @keys;

    foreach my $k (@keys){
        return $k;
    }
}

#!convertHashes
#Converts hash array into regular array
#specific to OPEN_SELL_ORDERS and OPEN_BUY_ORDERS

sub convertHashes{
    foreach my $price (sort keys %{$OPEN_ORDERS->{"S"}}){
        if(!defined $OPEN_ORDERS->{"S"}->{$price}){
            next;
        }
        my $total = 0;
        my $count = 0;
        foreach my $id (keys %{$OPEN_ORDERS->{"S"}->{$price}}){
            my @line = split(' ', $OPEN_ORDERS->{"S"}->{$price}->{$id});
            
            $total = $total + $line[1];
            $count = $count + 1;
        }
        
        if($count != 0){
            push @OPEN_SELL_ORDERS, "$price $total $count";
        }
        
    }

    foreach my $price (sort keys %{$OPEN_ORDERS->{"B"}}){
        if(!defined $OPEN_ORDERS->{"B"}->{$price}){
            next;
        }
        my $total = 0;
        my $count = 0;
        foreach my $id (keys %{$OPEN_ORDERS->{"B"}->{$price}}){
            my @line = split(' ', $OPEN_ORDERS->{"B"}->{$price}->{$id});
            $total = $total + $line[1];
            $count = $count + 1;
        }
        if($count != 0){
            push @OPEN_BUY_ORDERS, "$price $total $count";
        }
        
    }

    #print "Done converting hashes.\n";
}

sub sortArrays{
    no warnings;
    @OPEN_SELL_ORDERS = reverse sort { $a <=> $b } @OPEN_SELL_ORDERS;
    @OPEN_BUY_ORDERS = reverse sort { $a <=> $b } @OPEN_BUY_ORDERS;
    use warnings;
    #print "Done sorting arrays.\n";
}
sub printBBOlist{
    print "timestamp,symbol,type,price,qty,flag,bid,bidsize,ask,asksize\n";
    foreach my $val (@BBO_UPDATED_ORDERS){
        print $val;
    }
}

sub exportBBOlist{
    open(BBO, '>', "$path/bbo.$ticker_ref.$date_ref") or die $!;
    my $format = "//timestamp,symbol,type,price,qty,side,bid,bidsize,ask,asksize\n";
    print BBO $format;
    foreach my $val (@BBO_UPDATED_ORDERS){
        print BBO $val;
    }
    close(BBO);
}

sub printOrderbook{
    my $counter = 0;
    my $levels = 10;
    printf("%10s\t%10s\t%10s\t\n", "COUNT" ,"PRICE" ,"VOLUME");
    printf("SELL========================================================================================\n");
    if(scalar @OPEN_SELL_ORDERS <= 10){
    	foreach my $val (@OPEN_SELL_ORDERS){
        	my @line = split(' ', $val);
        	printf("%10d\t%.2f\t%10d\t\n", $line[2], convertPrice($line[0]), $line[1]);
        	$counter = $counter + 1;
    	}
    }else{
    	for (my $i = $levels * -1; $i <= -1; $i++){
		my @line = split(' ', $OPEN_SELL_ORDERS[$i]);
		printf("%10d\t%.2f\t%10d\t\n", $line[2], convertPrice($line[0]), $line[1]);
    	}
    }
    $counter = 0;
    printf("BUY========================================================================================\n");

    foreach my $val (@OPEN_BUY_ORDERS){
        if($counter == 10){
            last;
        }
        my @line = split(' ', $val);
        printf("%10d\t%.2f\t%10d\t\n", $line[2], convertPrice($line[0]), $line[1]);
        $counter = $counter + 1;
    }
}

#!ProcessLine function
sub processLine{
    my $line = shift;
    #0 trade, 1 order
    my $type = shift;

    if($type == 0){
        #Line is a trade line
        #Process trade lines
        #Check if line is a cancel or a execution
        my $BBO = getBBO();
        my $BSO = getBSO();
        if($line =~ /(\d{4})\,(\d{8,9})\,(\d+)\,(\d+)\,(\d+)\,(\d+)\,((S|B))\,(0)\,(0)\,(\d+)\,(\d+)/){
            ($tradeTicker, $tradeTime, $nID, $tradePrice, $tradeVolume, $turnover, $tradeBSflag, $orderKind, $functionCode, $askOrder, $bidOrder) = ($1, $2, $3, $4, $5, $6, $7, $9, $10, $11, $12);
            
            #print "$tradeTicker, $tradeTime, $nID, $tradePrice, $tradeVolume, $turnover, $tradeBSflag, $orderKind, $functionCode, $askOrder, $bidOrder\n";
            if(($tradeTime >= 93000000 && $tradeTime <= 113000000) || ($tradeTime >= 130000000 && $tradeTime <= 150000000) || ($tradeTime >= 91500000 && $tradeTime <= 92500000)){
                $tradeTime = convertTime($tradeTime);
                if($debug){
                    print "Order executed.\n";
                }
                my @sell_order_info = split(' ', findOrder($askOrder));
                my @buy_order_info = split(' ', findOrder($bidOrder));
                if($tradeBSflag eq "B"){

                    if($sell_order_info[1] == $tradeVolume){
                        #FULL EXECUTION
                        delete $OPEN_ORDERS->{"S"}->{$sell_order_info[0]}->{$askOrder};
                    }else{
                        #PARTIAL EXECUTION
                        my $updated_volume = $sell_order_info[1] - $tradeVolume;
                        $OPEN_ORDERS->{"S"}->{$sell_order_info[0]}->{$askOrder} = "$sell_order_info[0] $updated_volume";
                    }

                    if($buy_order_info[1] == $tradeVolume){
                        delete $OPEN_ORDERS->{"B"}->{$buy_order_info[0]}->{$bidOrder};
                    }else{
                        my $updated_volume = $buy_order_info[1] - $tradeVolume;
                        $OPEN_ORDERS->{"B"}->{$buy_order_info[0]}->{$bidOrder} = "$buy_order_info[0] $updated_volume";
                    }

                    if($tradePrice == $BSO){
                        $BBO = getBBO();
                        $BSO = getBSO();
                        $tradePrice = convertPrice($tradePrice);
			if(!($BBO >= $BSO)){
                            if($debug){
                                print "Added to BBO list : ";
                                print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                print "Added Quote to BBO list : ";
				print "$tradeTime,$tradeTicker,Q,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            }
                            push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,Q,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                        }else{
			    if($debug){
				print "Added to BBO list : ";
				print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
			    }
			    push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
			}
                        
                    }

                }else{    

                    if($buy_order_info[1] == $tradeVolume){
                        #FULL EXECUTION
                        delete $OPEN_ORDERS->{"B"}->{$buy_order_info[0]}->{$bidOrder};
                    }else{
                        #PARTIAL EXECUTION
                        my $updated_volume = $buy_order_info[1] - $tradeVolume;
                        $OPEN_ORDERS->{"B"}->{$buy_order_info[0]}->{$bidOrder} = "$buy_order_info[0] $updated_volume";
                    }

                    if($sell_order_info[1] == $tradeVolume){
                        delete $OPEN_ORDERS->{"S"}->{$sell_order_info[0]}->{$askOrder};
                    }else{
                        my $updated_volume = $sell_order_info[1] - $tradeVolume;
                        $OPEN_ORDERS->{"S"}->{$sell_order_info[0]}->{$askOrder} = "$sell_order_info[0] $updated_volume";
                    }
                    if($tradePrice == $BBO){
                        $BBO = getBBO();
                        $BSO = getBSO();
			$tradePrice = convertPrice($tradePrice);
                        if(!($BBO >= $BSO)){
                            if($debug){
                                print "Added to BBO list : ";
                                print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
				print "Added Quote to BBO list : ";                                
				print "$tradeTime,$tradeTicker,Q,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            }
                            push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,Q,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                        }else{
			    if($debug){
				print "Added to BBO list : ";
				print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
			    }
			    push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$tradeBSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
			}
                        
                    }
                }
            }
        }

        if($line =~ /(\d{4})\,(\d{8,9})\,(\d+)\,(\d+)\,(\d+)\,(\d+)\,(\s)\,(0)\,(C)\,(\d+)\,(\d+)/){
            ($tradeTicker, $tradeTime, $nID, $tradePrice, $tradeVolume, $turnover, $tradeBSflag, $orderKind, $functionCode, $askOrder, $bidOrder) = ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
            
            if(($tradeTime >= 93000000 && $tradeTime <= 113000000) || ($tradeTime >= 130000000 && $tradeTime <= 150000000) || ($tradeTime >= 91500000 && $tradeTime <= 92500000)){
                $tradeTime = convertTime($tradeTime);
                foreach my $flag (keys %{$OPEN_ORDERS}){
                    foreach my $price (keys %{$OPEN_ORDERS->{$flag}}){
                        foreach my $id (keys %{$OPEN_ORDERS->{$flag}->{$price}}){
                            if($id == $askOrder || $id == $bidOrder){
                                if(!defined $OPEN_ORDERS->{$flag}->{$price}->{$id}){
                                    $nonexistentcancel = $nonexistentcancel + 1;
                                    if($debug){
                                        print "Order ID was nonexistent!!! Total: ";
                                        print $nonexistentcancel;
                                        print "ID: ";
                                        print $id;
                                        print "\n";
                                    }
                                }
                                delete $OPEN_ORDERS->{$flag}->{$price}->{$id};
                                if($debug){
                                    print "Order cancelled.\n";
                                }
                                
                                if($price == $BBO || $price == $BSO){
                                    $BBO = getBBO();
                                    $BSO = getBSO();
				    $price = convertPrice($price);
                                    if($debug){
                                        print "Added Quote to BBO list : ";
                                        print "$tradeTime,$tradeTicker,Q,$price,$tradeVolume,$flag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                    }
                                    push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,Q,$price,$tradeVolume,$flag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                }

                                
                                return;
                            }
                        }
                    }
                }
                $nonexistentcancel = $nonexistentcancel + 1;
                print "Order ID was nonexistent!!! Total: ";
                print $nonexistentcancel;
                print "ID: ";
                print $id;
                print "\n";
            }
        }

    }else{
        #Line is an order line
        #process order lines
        if($line =~ /(\d{4})\,(\d{8,9})\,(\d+)\,(\d+)\,(\d+)\,(\w)\,(\w)/){
            ($ticker, $time, $id, $price, $volume, $otype, $BSflag) = ($1, $2, $3, $4, $5, $6, $7);
            #print "$ticker, $time, $id, $price, $volume, $otype, $BSflag\n";
            my $BBO = getBBO();
            my $BSO = getBSO();
            if(($time >= 93000000 && $time <= 113000000) || ($time >= 130000000 && $time <= 150000000) || ($time >= 91500000 && $time <= 92500000)){    
                $time = convertTime($time);
                if($otype eq "U"){
                    if($debug){
                        print "BBO order added.\n";
                    }
                    if($BSflag eq "B"){
                        $OPEN_ORDERS->{"B"}->{$price}->{$id} = "$price $volume";
                    }else{
                        $OPEN_ORDERS->{"S"}->{$price}->{$id} = "$price $volume";
                    }
                    $BBO = getBBO();
                    $BSO = getBSO();
		    $price = convertPrice($price);
                    if($debug){
                        print "Added Quote to BBO list : ";
                        print "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                    }
                    push @BBO_UPDATED_ORDERS, "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                    
                    
                }elsif($otype eq "1"){
                    if($debug){
                        print "Market order added.\n";
                    }
                    if($BSflag eq "B"){
                        # print "BSO: ";
                        # print getBSO();
                        # print "\n";
                        
                        $OPEN_ORDERS->{"B"}->{$BBO}->{$id} = "$BBO $volume";
                        $BBO = getBBO();
                        $BSO = getBSO();
                        if($debug){
                            print "Added to BBO list : ";
                            print "$time,$ticker,Q," . convertPrice($BBO) . ",$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                        }
                        push @BBO_UPDATED_ORDERS, "$time,$ticker,Q," . convertPrice($BBO) . ",$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                    }else{
                        # print "BBO: ";
                        # print getBBO();
                        # print "\n";
                        $OPEN_ORDERS->{"S"}->{$BSO}->{$id} = "$BSO $volume";
                        $BBO = getBBO();
                        $BSO = getBSO();
                        if($debug){
                            print "Added Quote to BBO list : ";
                            print "$time,$ticker,Q," . convertPrice($BSO) . ",$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                        }
                        push @BBO_UPDATED_ORDERS, "$time,$ticker,Q," . convertPrice($BSO) . ",$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                        
                    }
                }else{
                    if($BSflag eq "S"){
                        $OPEN_ORDERS->{"S"}->{$price}->{$id} = "$price $volume";
                        if($firstsell == 1){
				$BBO = getBBO();
				$BSO = getBSO();
				$price = convertPrice($price);
                                if($debug){
                                    print "Added Quote to BBO list : ";
                                    print "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                }
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                $firstsell = 0;
                                return;
                        }
                        if($price == $BBO || $price <= $BSO){
                            $BBO = getBBO();
                            $BSO = getBSO();
			    $price = convertPrice($price);
                            if($BBO >= $BSO){
                                if($debug){
                                    print "Order added.\n";
                                }
                                return;
                            }
                            if(!($BBO == $BSO)){
                                if($debug){
                                    print "Added Quote to BBO list : ";
                                    print "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                }
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            }

                            

                        }
                        if($debug){
                            print "Order added.\n";
                        }

                    }
                    if($BSflag eq "B"){
                        $OPEN_ORDERS->{"B"}->{$price}->{$id} = "$price $volume";
                        if($firstbuy == 1){
				$BBO = getBBO();
				$BSO = getBSO();
				$price = convertPrice($price);
                                if($debug){
                                    print "Added Quote BBO list : ";
                                    print "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                }
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                $firstbuy = 0;
                                return;
                        }
                        if($price >= $BBO || $price == $BSO){
                            
                            $BBO = getBBO();
                            $BSO = getBSO();
			    $price = convertPrice($price);

                            if($BBO >= $BSO){
                                if($debug){
                                    print "Order added.\n";
                                }
                                return;
                            }

                            if(!($BBO == $BSO)){
                                if($debug){
                                    print "Added Quote to BBO list : ";
                                    print "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                                }
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,Q,$price,$volume,$BSflag," . convertPrice($BBO) . "," . getTotal("B", $BBO) . "," . convertPrice($BSO) . "," . getTotal("S", $BSO) . "\n";
                            }
                            

                        }
                        if($debug){
                            print "Order added.\n";
                        }
                    }
                }
            }
        }

    }
}

#!Byte-address index used to seek line functions
sub build_index{
    my $data_file = shift;
    my $index_file = shift;
    my $offset = 0;

    while(<$data_file>) {
        print $index_file pack("N", $offset);
        $offset = tell($data_file);
    }
}

sub line_at_index{
    my $data_file = shift;
    my $index_file = shift;
    my $line_number = shift;

    my $size;
    my $i_offset;
    my $entry;
    my $d_offset;

    $size = length(pack("N", 0));
    $i_offset = $size * ($line_number - 1);
    seek($index_file, $i_offset, 0) or return;
    read($index_file, $entry, $size);
    $d_offset = unpack("N", $entry);
    if(defined $d_offset){
        seek($data_file, $d_offset, 0);
    }else{
        return undef;
    }
    
    return scalar(<$data_file>);
}

sub gen_orderbook{
    convertHashes();
    sortArrays();
    #print "@OPEN_BUY_ORDERS\n@OPEN_SELL_ORDERS\n";
    printOrderbook();
    @OPEN_BUY_ORDERS = ();
    @OPEN_SELL_ORDERS = ();
}



#Line merging logic

my $pointer = 0;

my $tradeline = 1;
my $orderline = 1;


open(ORDERS, "<$orderfile") or die "Could not open .order file $!";
open(TRADES, "<$tradefile") or die "Could not open .trade file $!";

open(T_INDEX, "+>t_index.db") or die "Could not open index.db for read/write $!";
open(O_INDEX, "+>o_index.db") or die "Could not open index.db for read/write $!";

build_index(*ORDERS, *O_INDEX);
build_index(*TRADES, *T_INDEX);


while((defined line_at_index(*ORDERS, *O_INDEX, $orderline)) || (defined line_at_index(*TRADES, *T_INDEX, $tradeline))){

    my $currTrade = line_at_index(*TRADES, *T_INDEX, $tradeline);
    my $currOrder = line_at_index(*ORDERS, *O_INDEX, $orderline);
    my @t_line;
    my @o_line;
    if(defined $currTrade){
        @t_line = split(',', $currTrade);
    }
    
    if(defined $currOrder){
        @o_line = split(',', $currOrder);
    }


    if(!defined $currOrder){
        
        $pointer = $t_line[1];
        if($debug){
            print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currTrade";
        }
        $tradeline++;
        processLine($currTrade, 0);
        if($debug){
            gen_orderbook();
            print "Press enter";
            <>;
        }
        next;
    }elsif(!defined $currTrade){
       
        $pointer = $o_line[1];
        if($debug){
            print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currOrder";
        }
        $orderline++;
        processLine($currOrder, 1);
        if($debug){
            gen_orderbook();
            print "Press enter";
            <>;
        }
        next;
    }
    
    if(($t_line[1] - $pointer) < ($o_line[1] - $pointer)){
        $pointer = $t_line[1];
        if($debug){
            print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currTrade";
        }
        $tradeline++;
        processLine($currTrade, 0);
        if($debug){
            gen_orderbook();
            print "Press enter";
            <>;
        }
    }else{
        $pointer = $o_line[1];
        if($debug){
            print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currOrder";
        }
        $orderline++;
        processLine($currOrder, 1);
        if($debug){
            gen_orderbook();
            print "Press enter";
            <>;
        }
    }
}

#print line_at_index(*TRADES, *T_INDEX, 173);
#print "\n";
# print Dumper \%{$OPEN_ORDERS};
# print "\n";
convertHashes();
sortArrays();
if($debug){
    printOrderbook();
}

#printBBOlist();
exportBBOlist();
#close files
close(ORDERS);
close(TRADES);
close(T_INDEX);
close(O_INDEX);
