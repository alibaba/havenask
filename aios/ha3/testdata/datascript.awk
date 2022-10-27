BEGIN{
    srand(1000);
}

{
    if(NR%3 == 0)
    {
	print "description="$0;
	print "id="int(NR/3) - 1;
	print "price="rand()*1000000;
	print "type=111";
	print "useid="(NR%15);
	print "</doc>\n\n<doc>";
    } 
    else if (NR%3 == 1)
    {
	print "title="$0;
    }
    else if(NR%3 == 2)
    { 
	print "body="$0;
    }
}