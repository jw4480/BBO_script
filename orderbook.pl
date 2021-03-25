use strict;
use warnings;
use POSIX;
use Getopt::Long;
use Data::Dumper;

#TODO
#Report error if Market order volume is bigger than total trade volume
#Count total of nonexistent order cancellations

#init argvs

#my $ticker_ref;
my $tradefile;
my $orderfile;
my $debug;

GetOptions (
            #'ticker:s' => \$ticker_ref,
            'tradefile:s' => \$tradefile,
            'orderfile:s' => \$orderfile,
            'debug' => \$debug,
          );

print "Trade file path : $tradefile\n";
print "Order file path : $orderfile\n";

#init vars

my $nonexistentcancel = 0;

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

    print "Done converting hashes.\n";
}

sub sortArrays{
    no warnings;
    @OPEN_SELL_ORDERS = reverse sort { $a <=> $b } @OPEN_SELL_ORDERS;
    @OPEN_BUY_ORDERS = reverse sort { $a <=> $b } @OPEN_BUY_ORDERS;
    use warnings;
    print "Done sorting arrays.\n";
}
sub printBBOlist{
    print "timestamp,symbol,type,price,qty,bid,bidsize,ask,asksize,bidRef,askRef,flag\n";
    foreach my $val (@BBO_UPDATED_ORDERS){
        print $val;
    }
}

sub exportBBOlist{
    open(BBO, '>', 'BBOlist') or die $!;
    foreach my $val (@BBO_UPDATED_ORDERS){
        print BBO $val;
    }
    close(BBO);
}

sub printOrderbook{
    printf("%10s\t%10s\t%10s\t\n", "COUNT" ,"PRICE" ,"VOLUME");
    printf("SELL========================================================================================\n");
    foreach my $val (@OPEN_SELL_ORDERS){
        my @line = split(' ', $val);
        printf("%10d\t%10d\t%10d\t\n", $line[2], $line[0], $line[1]);
    }

    printf("BUY========================================================================================\n");

    foreach my $val (@OPEN_BUY_ORDERS){
        my @line = split(' ', $val);
        printf("%10d\t%10d\t%10d\t\n", $line[2], $line[0], $line[1]);
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
                print "Order executed.\n";
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

                    if($price >= $BSO){
                        $BBO = getBBO();
                        $BSO = getBSO();
                        if($BBO >= $BSO){
                            return;
                        }
                        print "Added to BBO list : ";
                        print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
                        push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
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
                    if($price <= $BBO){
                        $BBO = getBBO();
                        $BSO = getBSO();
                        if($BBO >= $BSO){
                            return;
                        }
                        print "Added to BBO list : ";
                        print "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
                        push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,T,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
                    }
                }
            }
        }

        if($line =~ /(\d{4})\,(\d{8,9})\,(\d+)\,(\d+)\,(\d+)\,(\d+)\,(\s)\,(0)\,(C)\,(\d+)\,(\d+)/){
            ($tradeTicker, $tradeTime, $nID, $tradePrice, $tradeVolume, $turnover, $tradeBSflag, $orderKind, $functionCode, $askOrder, $bidOrder) = ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
            
            if(($tradeTime >= 93000000 && $tradeTime <= 113000000) || ($tradeTime >= 130000000 && $tradeTime <= 150000000) || ($tradeTime >= 91500000 && $tradeTime <= 92500000)){
                foreach my $flag (keys %{$OPEN_ORDERS}){
                    foreach my $price (keys %{$OPEN_ORDERS->{$flag}}){
                        foreach my $id (keys %{$OPEN_ORDERS->{$flag}->{$price}}){
                            if($id == $askOrder || $id == $bidOrder){
                                if(!defined $OPEN_ORDERS->{$flag}->{$price}->{$id}){
                                    $nonexistentcancel = $nonexistentcancel + 1;
                                    print "Order ID was nonexistent!!! Total: ";
                                    print $nonexistentcancel;
                                    print "ID: ";
                                    print $id;
                                    print "\n";
                                }
                                delete $OPEN_ORDERS->{$flag}->{$price}->{$id};

                                print "Order cancelled.\n";
                                if($price == $BBO || $price == $BSO){
                                    $BBO = getBBO();
                                    $BSO = getBSO();
                                    print "Added to BBO list : ";
                                    print "$tradeTime,$tradeTicker,C,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
                                    push @BBO_UPDATED_ORDERS, "$tradeTime,$tradeTicker,C,$tradePrice,$tradeVolume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$bidOrder,$askOrder,$tradeBSflag\n";
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
                if($otype eq "U"){
                    print "BBO order added.\n";
                    if($BSflag eq "B"){
                        $OPEN_ORDERS->{"B"}->{$price}->{$id} = "$price $volume";
                    }else{
                        $OPEN_ORDERS->{"S"}->{$price}->{$id} = "$price $volume";
                    }
                    $BBO = getBBO();
                    $BSO = getBSO();
                    print "Added to BBO list : ";
                    print "$time,$ticker,U,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                    push @BBO_UPDATED_ORDERS, "$time,$ticker,U,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                    
                    
                }elsif($otype eq "1"){
                    print "Market order added.\n";
                    
                    if($BSflag eq "B"){
                        # print "BSO: ";
                        # print getBSO();
                        # print "\n";
                        $OPEN_ORDERS->{"B"}->{"0"}->{$id} = "0 $volume";
                        # $BBO = getBBO();
                        # $BSO = getBSO();
                        # print "Added to BBO list : ";
                        # print "$time,$ticker,M,$BSO,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                        # push @BBO_UPDATED_ORDERS, "$time,$ticker,M,$BSO,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                    }else{
                        # print "BBO: ";
                        # print getBBO();
                        # print "\n";
                        $OPEN_ORDERS->{"S"}->{"0"}->{$id} = "0 $volume";
                        # $BBO = getBBO();
                        # $BSO = getBSO();
                        # print "Added to BBO list : ";
                        # print "$time,$ticker,M,$BBO,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                        # push @BBO_UPDATED_ORDERS, "$time,$ticker,M,$BBO,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                        
                    }
                }else{
                    if($BSflag eq "S"){
                        $OPEN_ORDERS->{"S"}->{$price}->{$id} = "$price $volume";
                        if($price == $BBO || $price <= $BSO){
                            $BBO = getBBO();
                            $BSO = getBSO();
                            if($BBO >= $BSO){
                                print "Order added.\n";
                                return;
                            }
                            if(!($BBO == $BSO)){
                                print "Added to BBO list : ";
                                print "$time,$ticker,O,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,O,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",0,$id,$BSflag\n";
                            }

                        }

                    }
                    if($BSflag eq "B"){
                        $OPEN_ORDERS->{"B"}->{$price}->{$id} = "$price $volume";
                        if($price >= $BBO || $price == $BSO){
                            
                            $BBO = getBBO();
                            $BSO = getBSO();

                            if($BBO >= $BSO){
                                print "Order added.\n";
                                return;
                            }

                            if(!($BBO == $BSO)){
                                print "Added to BBO list : ";
                                print "$time,$ticker,O,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$id,0,$BSflag\n";
                                push @BBO_UPDATED_ORDERS, "$time,$ticker,O,$price,$volume,$BBO," . getTotal("B", $BBO) . ",$BSO," . getTotal("S", $BSO) . ",$id,0,$BSflag\n";
                            }

                        }
                    }
                    print "Order added.\n";
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
        print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currTrade";
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
        print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currOrder";
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
        
        print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currTrade";
        $tradeline++;
        processLine($currTrade, 0);
        if($debug){
            gen_orderbook();
            print "Press enter";
            <>;
        }
    }else{
        $pointer = $o_line[1];
        
        print "Pointer: $pointer OLINE: $orderline TLINE: $tradeline Line: $currOrder";
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
print Dumper \%{$OPEN_ORDERS};
print "\n";
convertHashes();
sortArrays();
printOrderbook();
#printBBOlist();
exportBBOlist();
#close files
close(ORDERS);
close(TRADES);
close(T_INDEX);
close(O_INDEX);