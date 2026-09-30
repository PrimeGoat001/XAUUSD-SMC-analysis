import os 
import time 
import threading 
from datetime import datetime ,timezone 
import requests 
from flask import Flask ,jsonify ,request ,render_template 

app =Flask (__name__ )
SYMBOL ="GC=F"
YAHOO_URL ="https://query1.finance.yahoo.com/v8/finance/chart/"+SYMBOL 
PRICE_OFFSET =+2.71 
HEADERS ={"User-Agent":"Mozilla/5.0"}
SESSION =requests .Session ()
SESSION .headers .update (HEADERS )
RANGE_MAP ={"1m":"5d","5m":"60d","15m":"60d","30m":"60d","1h":"1mo","1d":"1y"}
VALID_TFS ={"1m","5m","15m","30m","1h","1d"}
YAHOO_INTERVAL_MAP ={"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"60m","1d":"1d"}
_cache ={}
_cache_lock =threading .Lock ()
CACHE_TTL =0.25 























_smc_signal_history ={}
_smc_signal_lock =threading .Lock ()


def _smc_get_signal_store (interval ):
    with _smc_signal_lock :

        if interval not in _smc_signal_history :

            _smc_signal_history [interval ]={
            "signals":[],

            
            
            
            "entries":{
            "BUY":None ,
            "SELL":None 
            },

            
            "last_direction":None ,

            
            "active_entry":None 
            }

        return _smc_signal_history [interval ]


def _smc_record_signal (
interval ,
direction ,
signal ,
candle ,
setup_id =None 
):
    """
    Record an SMC signal only when it represents a genuinely new setup.

    Same direction + same underlying setup:
        BUY  -> BUY  = ignore
        SELL -> SELL = ignore

    Same direction + genuinely new confirmed setup:
        BUY  -> new BUY setup  = accept
        SELL -> new SELL setup = accept

    Opposite direction + genuinely new confirmed setup:
        BUY  -> SELL = accept
        SELL -> BUY  = accept

    Setup identity is based on the confirmed SMC event/context rather than
    direction alone, including structure, liquidity, displacement, FVG/OB
    and trendline evidence where available.

    Accepted markers retain the exact original candle timestamp and exact
    signal/entry price. Historical markers are never moved or deleted.
    """

    if not candle :
        return None 

    direction =str (
    direction or ""
    ).upper ()

    if direction not in {
    "BUY",
    "SELL"
    }:
        return None 

    signal_time =candle .get ("time")

    signal_price =safe_float (
    candle .get ("close"),
    0.0 
    )

    if signal_time is None :
        return None 

    signal_time =int (signal_time )

    with _smc_signal_lock :

        if interval not in _smc_signal_history :

            _smc_signal_history [interval ]={
            "signals":[],
            "entries":{
            "BUY":None ,
            "SELL":None 
            },
            "last_direction":None ,
            "last_setup_id":None ,
            "active_entry":None 
            }

        store =_smc_signal_history [
        interval 
        ]

        last_direction =store .get (
        "last_direction"
        )
        last_setup_id =store .get ("last_setup_id")

        
        
        
        
        
        
        
        
        

        if last_direction ==direction and setup_id and last_setup_id ==setup_id :
            return None 

            
        if last_direction ==direction and not setup_id :
            return None 

            
            
            
            
            

        marker ={
        "time":signal_time ,
        "price":round (
        signal_price ,
        2 
        ),
        "signal":signal ,
        "direction":direction ,
        "setup_id":setup_id 
        }

        
        
        
        
        
        
        

        existing =any (
        item .get ("time")==signal_time 
        and 
        item .get ("direction")==direction 
        for item in store ["signals"]
        )

        if existing :
            return None 

            
            
            

        store ["signals"].append (
        marker 
        )

        
        
        
        
        

        store ["entries"][direction ]=dict (
        marker 
        )

        
        
        
        
        
        

        store ["active_entry"]=dict (
        marker 
        )

        store ["last_direction"]=direction 
        store ["last_setup_id"]=setup_id 

        return dict (marker )


def _smc_get_historical_signals (interval ):
    with _smc_signal_lock :

        store =_smc_signal_history .get (
        interval ,
        {
        "signals":[],
        "entries":{
        "BUY":None ,
        "SELL":None 
        },
        "last_direction":None ,
        "last_setup_id":None ,
        "active_entry":None 
        }
        )

        return {
        "signals":[
        dict (item )
        for item in store .get (
        "signals",
        []
        )
        ],

        "entries":{
        direction :(
        dict (entry )
        if entry 
        else None 
        )
        for direction ,entry 
        in store .get (
        "entries",
        {
        "BUY":None ,
        "SELL":None 
        }
        ).items ()
        },

        "last_direction":store .get (
        "last_direction"
        ),

        "active_entry":(
        dict (
        store ["active_entry"]
        )
        if store .get (
        "active_entry"
        )
        else None 
        )
        }


def safe_float (v ,d =0.0 ):
    try :
        return float (v )if v is not None else d 
    except :
        return d 


def clamp (v ,mi ,ma ):
    return max (mi ,min (ma ,v ))


def now_utc_iso ():
    return datetime .now (timezone .utc ).isoformat ()


def is_market_open ():
    now =datetime .now (timezone .utc )
    d ,h =now .weekday (),now .hour 

    if d ==5 :
        return False 

    if d ==4 and h >=21 :
        return False 

    if d ==6 and h <22 :
        return False 

    return True 


def normalize_tf (tf ):
    tf =str (tf or "5m").lower ().strip ()

    aliases ={
    "1":"1m",
    "1m":"1m",
    "5":"5m",
    "5m":"5m",
    "15":"15m",
    "15m":"15m",
    "30":"30m",
    "30m":"30m",
    "30min":"30m",
    "60":"1h",
    "1h":"1h",
    "1hr":"1h",
    "hour":"1h",
    "2h":"1h",
    "4h":"1h",
    "45m":"30m",
    "day":"1d",
    "daily":"1d",
    "1d":"1d"
    }

    tf =aliases .get (tf ,tf )

    return tf if tf in VALID_TFS else "5m"


def get_yahoo_raw_close ():
    try :
        params ={
        "interval":"5m",
        "range":"5d",
        "includePrePost":"false"
        }

        r =SESSION .get (
        YAHOO_URL ,
        params =params ,
        timeout =4 
        )

        r .raise_for_status ()

        closes =r .json ()["chart"]["result"][0 ]["indicators"]["quote"][0 ]["close"]
        closes =[c for c in closes if c is not None ]

        if closes :
            return float (closes [-1 ])

    except :
        pass 

    return None 


def get_spot_live ():
    try :
        r =SESSION .get (
        "https://api.gold-api.com/price/XAU",
        timeout =2 
        )

        if r .status_code ==200 :
            p =float (r .json ().get ("price",0 ))

            if p >1000 :
                return p 

    except :
        pass 

    return None 


def auto_sync_loop ():
    global PRICE_OFFSET 

    while True :
        try :
            y_raw =get_yahoo_raw_close ()
            spot =get_spot_live ()

            if y_raw and spot :
                new_off =spot -y_raw 

                
                
                
                if -100 <new_off <100 :
                    new_off =round (new_off ,2 )

                    if new_off !=PRICE_OFFSET :
                        PRICE_OFFSET =new_off 

                        
                        
                        with _cache_lock :
                            _cache .clear ()

            time .sleep (2 )

        except :
            time .sleep (2 )


threading .Thread (
target =auto_sync_loop ,
daemon =True 
).start ()


def get_gold (interval ="5m"):
    interval =normalize_tf (interval )
    now =time .time ()

    with _cache_lock :
        if interval in _cache :
            c ,t =_cache [interval ]

            if now -t <CACHE_TTL :
                return c 

    tf_range =RANGE_MAP [interval ]
    yahoo_interval =YAHOO_INTERVAL_MAP .get (
    interval ,
    interval 
    )

    params ={
    "interval":yahoo_interval ,
    "range":tf_range ,
    "includePrePost":"false",
    "events":"div,splits"
    }

    try :
        response =SESSION .get (
        YAHOO_URL ,
        params =params ,
        timeout =5 
        )

        response .raise_for_status ()

        result =response .json ()["chart"]["result"][0 ]

        timestamps =result .get (
        "timestamp",
        []
        )

        quote =result .get (
        "indicators",
        {}
        ).get (
        "quote",
        [{}]
        )[0 ]

        opens =quote .get ("open",[])
        highs =quote .get ("high",[])
        lows =quote .get ("low",[])
        closes =quote .get ("close",[])
        volumes =quote .get ("volume",[])

        
        
        
        
        global PRICE_OFFSET 
        raw_closes =[
        float (value )
        for value in closes 
        if value is not None 
        ]

        if raw_closes :
            live_spot =get_spot_live ()

            if live_spot and live_spot >1000 :
                live_offset =round (
                live_spot -raw_closes [-1 ],
                2 
                )

                if -100 <live_offset <100 :
                    PRICE_OFFSET =live_offset 

        candles =[]

        length =min (
        len (timestamps ),
        len (opens ),
        len (highs ),
        len (lows ),
        len (closes )
        )

        for i in range (length ):

            o ,h ,l ,c =(
            opens [i ],
            highs [i ],
            lows [i ],
            closes [i ]
            )

            if None in (o ,h ,l ,c ):
                continue 

            candles .append ({
            "time":int (timestamps [i ]),

            
            
            "open":round (
            float (o )+PRICE_OFFSET ,
            2 
            ),

            "high":round (
            float (h )+PRICE_OFFSET ,
            2 
            ),

            "low":round (
            float (l )+PRICE_OFFSET ,
            2 
            ),

            "close":round (
            float (c )+PRICE_OFFSET ,
            2 
            ),

            "volume":int (volumes [i ])
            if i <len (volumes )
            and volumes [i ]is not None 
            else 0 
            })

            
        if not candles and interval =="1m":

            params_fallback ={
            "interval":"5m",
            "range":"5d",
            "includePrePost":"false",
            "events":"div,splits"
            }

            response_fb =SESSION .get (
            YAHOO_URL ,
            params =params_fallback ,
            timeout =5 
            )

            response_fb .raise_for_status ()

            result_fb =response_fb .json ()["chart"]["result"][0 ]

            timestamps =result_fb .get (
            "timestamp",
            []
            )

            quote_fb =result_fb .get (
            "indicators",
            {}
            ).get (
            "quote",
            [{}]
            )[0 ]

            opens =quote_fb .get ("open",[])
            highs =quote_fb .get ("high",[])
            lows =quote_fb .get ("low",[])
            closes =quote_fb .get ("close",[])
            volumes =quote_fb .get ("volume",[])

            length =min (
            len (timestamps ),
            len (opens ),
            len (highs ),
            len (lows ),
            len (closes )
            )

            for i in range (length ):

                o ,h ,l ,c =(
                opens [i ],
                highs [i ],
                lows [i ],
                closes [i ]
                )

                if None in (o ,h ,l ,c ):
                    continue 

                candles .append ({
                "time":int (timestamps [i ]),

                "open":round (
                float (o )+PRICE_OFFSET ,
                2 
                ),

                "high":round (
                float (h )+PRICE_OFFSET ,
                2 
                ),

                "low":round (
                float (l )+PRICE_OFFSET ,
                2 
                ),

                "close":round (
                float (c )+PRICE_OFFSET ,
                2 
                ),

                "volume":int (volumes [i ])
                if i <len (volumes )
                and volumes [i ]is not None 
                else 0 
                })

        result_candles =candles [-250 :]

        with _cache_lock :
            _cache [interval ]=(
            result_candles ,
            time .time ()
            )

        return result_candles 

    except Exception as exc :
        print (
        f"[DATA ERROR] {interval }: {exc }"
        )

        return []


_last_live_price =0 
_last_live_fetch =0 


def get_oanda_live ():
    global _last_live_price ,_last_live_fetch 

    if (
    time .time ()-_last_live_fetch <0.8 
    and _last_live_price 
    ):
        return _last_live_price 

    try :
        r =SESSION .get (
        "https://api.gold-api.com/price/XAU",
        timeout =2 
        )

        if r .status_code ==200 :

            p =float (
            r .json ().get (
            "price",
            0 
            )
            )

            if p >1000 :
                _last_live_price =p 
                _last_live_fetch =time .time ()

                return _last_live_price 

    except :
        pass 

    return _last_live_price 


def get_gold_with_live (interval ="5m"):
    """
    Return the same Yahoo candle stream used by get_gold().

    The chart must not mix Yahoo historical OHLC with a separate
    Gold-API spot price inside the latest candle. Doing so can create
    an artificial vertical candle whenever the two feeds differ.

    The existing PRICE_OFFSET remains applied by get_gold().
    """
    candles =get_gold (interval )

    
    
    return [dict (candle )for candle in candles ]

def calculate_rsi (closes ,period =14 ):
    if len (closes )<=period :
        return 50.0 

    gains =[]
    losses =[]

    for i in range (1 ,len (closes )):
        ch =closes [i ]-closes [i -1 ]

        gains .append (
        max (ch ,0 )
        )

        losses .append (
        max (-ch ,0 )
        )

    avg_gain =sum (
    gains [:period ]
    )/period 

    avg_loss =sum (
    losses [:period ]
    )/period 

    for i in range (
    period ,
    len (gains )
    ):
        avg_gain =(
        (
        avg_gain *
        (period -1 )
        )+
        gains [i ]
        )/period 

        avg_loss =(
        (
        avg_loss *
        (period -1 )
        )+
        losses [i ]
        )/period 

    if avg_loss ==0 :
        return 100.0 

    rs =avg_gain /avg_loss 

    return round (
    100 -
    (
    100 /
    (1 +rs )
    ),
    2 
    )


def calculate_atr (candles ,period =14 ):
    if len (candles )<period +1 :
        return 0.0 

        
        
        
    start =max (
    1 ,
    len (candles )-period 
    )

    true_ranges =[]

    for i in range (
    start ,
    len (candles )
    ):
        c =candles [i ]
        p =candles [i -1 ]

        tr =max (
        c ["high"]-c ["low"],
        abs (
        c ["high"]-
        p ["close"]
        ),
        abs (
        c ["low"]-
        p ["close"]
        )
        )

        true_ranges .append (tr )

    return (
    round (
    sum (true_ranges )/period ,
    4 
    )
    if true_ranges 
    else 0.0 
    )


def find_swing_highs (
candles ,
left =2 ,
right =2 
):
    res =[]

    
    
    
    if left ==2 and right ==2 :
        for i in range (
        2 ,
        len (candles )-2 
        ):
            h =candles [i ]["high"]

            if (
            h >=candles [i -2 ]["high"]
            and 
            h >=candles [i -1 ]["high"]
            and 
            h >candles [i +1 ]["high"]
            and 
            h >candles [i +2 ]["high"]
            ):
                res .append (i )

        return res 

    for i in range (
    left ,
    len (candles )-right 
    ):
        h =candles [i ]["high"]

        if (
        h >=max (
        candles [j ]["high"]
        for j in range (
        i -left ,
        i 
        )
        )
        and 
        h >max (
        candles [j ]["high"]
        for j in range (
        i +1 ,
        i +right +1 
        )
        )
        ):
            res .append (i )

    return res 


def find_swing_lows (
candles ,
left =2 ,
right =2 
):
    res =[]

    
    
    
    if left ==2 and right ==2 :
        for i in range (
        2 ,
        len (candles )-2 
        ):
            l =candles [i ]["low"]

            if (
            l <=candles [i -2 ]["low"]
            and 
            l <=candles [i -1 ]["low"]
            and 
            l <candles [i +1 ]["low"]
            and 
            l <candles [i +2 ]["low"]
            ):
                res .append (i )

        return res 

    for i in range (
    left ,
    len (candles )-right 
    ):
        l =candles [i ]["low"]

        if (
        l <=min (
        candles [j ]["low"]
        for j in range (
        i -left ,
        i 
        )
        )
        and 
        l <min (
        candles [j ]["low"]
        for j in range (
        i +1 ,
        i +right +1 
        )
        )
        ):
            res .append (i )

    return res 


def get_single_tf_bias (tf ):
    candles =get_gold (tf )

    if len (candles )<30 :
        return "NEUTRAL"

    sh =find_swing_highs (
    candles ,
    2 ,
    2 
    )

    sl =find_swing_lows (
    candles ,
    2 ,
    2 
    )

    if len (sh )<2 or len (sl )<2 :
        return "NEUTRAL"

    last_high =candles [
    sh [-1 ]
    ]["high"]

    prev_high =candles [
    sh [-2 ]
    ]["high"]

    last_low =candles [
    sl [-1 ]
    ]["low"]

    prev_low =candles [
    sl [-2 ]
    ]["low"]

    price =candles [-1 ]["close"]

    if (
    last_high >prev_high 
    and 
    last_low >prev_low 
    and 
    price >=last_low 
    ):
        return "BULLISH"

    if (
    last_high <prev_high 
    and 
    last_low <prev_low 
    and 
    price <=last_high 
    ):
        return "BEARISH"

    if price >last_high :
        return "BULLISH"

    if price <last_low :
        return "BEARISH"

    return "NEUTRAL"


def build_mtf_matrix ():
    return {
    "1m":get_single_tf_bias ("1m"),
    "5m":get_single_tf_bias ("5m"),
    "15m":get_single_tf_bias ("15m"),
    "30m":get_single_tf_bias ("30m"),
    "1h":get_single_tf_bias ("1h"),
    "1d":get_single_tf_bias ("1d")
    }


def detect_structure (candles ):
    if len (candles )<20 :
        return {
        "state":"NEUTRAL",
        "event":None ,
        "level":None ,
        "index":None 
        }

    sh =find_swing_highs (
    candles ,
    2 ,
    2 
    )

    sl =find_swing_lows (
    candles ,
    2 ,
    2 
    )

    if not sh or not sl :
        return {
        "state":"NEUTRAL",
        "event":None ,
        "level":None ,
        "index":None 
        }

    last_high_idx =sh [-1 ]
    last_low_idx =sl [-1 ]

    last_high =candles [
    last_high_idx 
    ]["high"]

    last_low =candles [
    last_low_idx 
    ]["low"]

    prev_state ="NEUTRAL"

    if len (sh )>=2 and len (sl )>=2 :

        h1 =candles [
        sh [-2 ]
        ]["high"]

        h2 =candles [
        sh [-1 ]
        ]["high"]

        l1 =candles [
        sl [-2 ]
        ]["low"]

        l2 =candles [
        sl [-1 ]
        ]["low"]

        if (
        h2 >h1 
        and 
        l2 >l1 
        ):
            prev_state ="BULLISH"

        elif (
        h2 <h1 
        and 
        l2 <l1 
        ):
            prev_state ="BEARISH"

    close =candles [-1 ]["close"]

    if close >last_high :

        return {
        "state":"BULLISH",
        "event":(
        "CHoCH"
        if prev_state =="BEARISH"
        else "BOS"
        ),
        "level":last_high ,
        "index":last_high_idx 
        }

    if close <last_low :

        return {
        "state":"BEARISH",
        "event":(
        "CHoCH"
        if prev_state =="BULLISH"
        else "BOS"
        ),
        "level":last_low ,
        "index":last_low_idx 
        }

    return {
    "state":prev_state ,
    "event":None ,
    "level":(
    last_high 
    if prev_state =="BULLISH"
    else 
    last_low 
    if prev_state =="BEARISH"
    else 
    None 
    ),
    "index":(
    last_high_idx 
    if prev_state =="BULLISH"
    else 
    last_low_idx 
    if prev_state =="BEARISH"
    else 
    None 
    )
    }


def detect_liquidity (
candles ,
tolerance_factor =0.12 
):
    if len (candles )<30 :
        return [],[]

    atr =calculate_atr (candles )or 1.0 
    tol =atr *tolerance_factor 

    sh =find_swing_highs (
    candles ,
    2 ,
    2 
    )

    sl =find_swing_lows (
    candles ,
    2 ,
    2 
    )

    lines =[]
    events =[]

    if len (sh )>=2 :

        a =sh [-1 ]
        b =sh [-2 ]

        p1 =candles [a ]["high"]
        p2 =candles [b ]["high"]

        if abs (p1 -p2 )<=tol :

            level =round (
            (p1 +p2 )/2 ,
            2 
            )

            if candles [-1 ]["high"]<=level :

                lines .append ({
                "price":level ,
                "color":"#00e5ff",
                "title":"EQUAL HIGH",
                "lineStyle":2 ,
                "layer":"liquidity",
                "active":True 
                })

            else :

                events .append ({
                "type":"BUY_SIDE_SWEEP",
                "price":level ,
                "time":candles [-1 ]["time"],
                "label":"BUY-SIDE SWEEP"
                })

    if len (sl )>=2 :

        a =sl [-1 ]
        b =sl [-2 ]

        p1 =candles [a ]["low"]
        p2 =candles [b ]["low"]

        if abs (p1 -p2 )<=tol :

            level =round (
            (p1 +p2 )/2 ,
            2 
            )

            if candles [-1 ]["low"]>=level :

                lines .append ({
                "price":level ,
                "color":"#00e5ff",
                "title":"EQUAL LOW",
                "lineStyle":2 ,
                "layer":"liquidity",
                "active":True 
                })

            else :

                events .append ({
                "type":"SELL_SIDE_SWEEP",
                "price":level ,
                "time":candles [-1 ]["time"],
                "label":"SELL-SIDE SWEEP"
                })

    return lines ,events 


    
    
    

def _zone_state (zone ,candles ):
    """Evaluate a zone using the agreed two-candle violation rule.

    - A wick through the zone is NOT a violation. If the candle closes back
      inside the zone (or on the valid side) the setup is valid/respected.
    - A candle that closes beyond the invalidation boundary is only a
      POTENTIAL violation. The next candle decides:
          closes beyond again -> zone violated
          closes back in/valid side -> zone stays valid
    - A candle that closes inside the zone is valid.
    """
    if not candles :
        return {"status":"invalid","confirmed":False ,"retested":False ,"respected":False ,"swept":False }

    top =safe_float (zone .get ("top"),None )
    bottom =safe_float (zone .get ("bottom"),None )
    direction =str (zone .get ("direction","")).lower ()
    created_time =zone .get ("createdTime")

    if top is None or bottom is None or created_time is None :
        return {"status":"invalid","confirmed":False ,"retested":False ,"respected":False ,"swept":False }

    post =[c for c in candles if c .get ("time",0 )>created_time ]
    if not post :
        return {"status":"valid","confirmed":False ,"retested":False ,"respected":False ,"swept":False }

    retested =False 
    respected =False 
    potential =False 
    swept =False 

    for idx ,candle in enumerate (post ):
        close =safe_float (candle .get ("close"),None )
        high =safe_float (candle .get ("high"),None )
        low =safe_float (candle .get ("low"),None )
        if close is None or high is None or low is None :
            continue 

        if direction =="bullish":
            touched =low <=top and high >=bottom 
            potential_break =close <bottom 
            wick_past =low <bottom and close >=bottom 
        elif direction =="bearish":
            touched =high >=bottom and low <=top 
            potential_break =close >top 
            wick_past =high >top and close <=top 
        else :
            return {"status":"invalid","confirmed":False ,"retested":False ,"respected":False ,"swept":False }

        if touched :
            retested =True 
            
            
            if not potential_break :
                respected =True 
            if wick_past :
                swept =True 

        if potential_break :
            potential =True 
            
            if idx +1 <len (post ):
                next_close =safe_float (post [idx +1 ].get ("close"),None )
                if next_close is not None :
                    if direction =="bullish":
                        if next_close <bottom :
                            return {"status":"violated","confirmed":True ,"retested":retested ,
                            "respected":respected ,"swept":swept ,
                            "violated_time":post [idx +1 ].get ("time")}
                        respected =True 
                        retested =True 
                        potential =False 
                    else :
                        if next_close >top :
                            return {"status":"violated","confirmed":True ,"retested":retested ,
                            "respected":respected ,"swept":swept ,
                            "violated_time":post [idx +1 ].get ("time")}
                        respected =True 
                        retested =True 
                        potential =False 

                        
    if potential :
        return {"status":"potential_violation","confirmed":False ,"retested":retested ,
        "respected":respected ,"swept":swept }
    if retested and respected :
        return {"status":"respected","confirmed":True ,"retested":True ,"respected":True ,"swept":swept }
    return {"status":"valid","confirmed":False ,"retested":retested ,"respected":respected ,"swept":swept }


def is_zone_violated (zone ,candles ,created_index =None ,confirm_bars =2 ):
    return _zone_state (zone ,candles ).get ("status")=="violated"


    
    
    

def is_fvg_filled (zone ,candles ):


    return _zone_state (zone ,candles ).get ("status")=="violated"


def _add_zone_state (zone ,candles ):
    state =_zone_state (zone ,candles )
    zone ["status"]=state ["status"]
    zone ["confirmed"]=bool (state ["confirmed"])
    zone ["retested"]=bool (state ["retested"])
    zone ["respected"]=bool (state ["respected"])
    zone ["swept"]=bool (state .get ("swept"))
    zone ["valid"]=state ["status"]!="violated"
    return zone 


def detect_fvgs (candles ,max_zones =6 ):
    active =[]
    if len (candles )<5 :
        return active 

    atr =calculate_atr (candles )or 1.0 
    min_gap =atr *0.10 
    start =max (2 ,len (candles )-80 )
    cands =[]

    for i in range (start ,len (candles )):
        left =candles [i -2 ]
        right =candles [i ]
        if right ["low"]>left ["high"]:
            gap =right ["low"]-left ["high"]
            if gap >=min_gap :
                cands .append ({
                "type":"Bullish FVG","label":"BULLISH FVG","direction":"bullish",
                "top":round (right ["low"],2 ),"bottom":round (left ["high"],2 ),
                "timeStart":left ["time"],"createdTime":right ["time"],
                "createdIndex":i ,"color":"rgba(0,255,136,0.16)",
                "borderColor":"#00ff88","layer":"fvg","valid":True 
                })
        elif right ["high"]<left ["low"]:
            gap =left ["low"]-right ["high"]
            if gap >=min_gap :
                cands .append ({
                "type":"Bearish FVG","label":"BEARISH FVG","direction":"bearish",
                "top":round (left ["low"],2 ),"bottom":round (right ["high"],2 ),
                "timeStart":left ["time"],"createdTime":right ["time"],
                "createdIndex":i ,"color":"rgba(255,68,68,0.16)",
                "borderColor":"#ff4444","layer":"fvg","valid":True 
                })

    for zone in cands :
        _add_zone_state (zone ,candles )
        if zone .get ("status")=="violated":
            continue 
        zone .pop ("createdIndex",None )
        active .append (zone )

    return active [-max_zones :]


def detect_order_blocks (candles ,max_zones =6 ):
    """Independent OB engine. OB validity never depends on an FVG."""
    active =[]
    if len (candles )<10 :
        return active 

    atr =calculate_atr (candles )or 1.0 
    avg_body =sum (abs (c ["close"]-c ["open"])for c in candles [-20 :])/min (20 ,len (candles ))
    start =max (2 ,len (candles )-80 )
    cands =[]

    for i in range (start ,len (candles )-1 ):
        cur =candles [i ]
        nxt =candles [i +1 ]
        body =abs (nxt ["close"]-nxt ["open"])
        
        if body <=avg_body *1.05 or body <=atr *0.20 :
            continue 

        if cur ["close"]<cur ["open"]and nxt ["close"]>cur ["high"]:
            cands .append ({
            "type":"Bullish Order Block","label":"BULLISH OB","direction":"bullish",
            "top":round (cur ["high"],2 ),"bottom":round (cur ["low"],2 ),
            "timeStart":cur ["time"],"createdTime":nxt ["time"],"createdIndex":i +1 ,
            "color":"rgba(34,197,94,0.18)","borderColor":"#22c55e","layer":"ob","valid":True 
            })
        elif cur ["close"]>cur ["open"]and nxt ["close"]<cur ["low"]:
            cands .append ({
            "type":"Bearish Order Block","label":"BEARISH OB","direction":"bearish",
            "top":round (cur ["high"],2 ),"bottom":round (cur ["low"],2 ),
            "timeStart":cur ["time"],"createdTime":nxt ["time"],"createdIndex":i +1 ,
            "color":"rgba(239,68,68,0.18)","borderColor":"#ef4444","layer":"ob","valid":True 
            })

    for zone in cands :
        _add_zone_state (zone ,candles )
        if zone .get ("status")=="violated":
            continue 
        zone .pop ("createdIndex",None )
        active .append (zone )

    return active [-max_zones :]

    
    
    

def is_fvg_filled (
zone ,
candles 
):


    return _zone_state (
    zone ,
    candles 
    ).get ("status")in {"violated","invalid"}


def detect_fvgs (
candles ,
max_zones =6 ,
violated_out =None 
):
    """Detect only meaningful wick-to-wick three-candle FVGs.

    Quality is based on gap size relative to ATR, middle-candle displacement,
    body strength and whether the formation is structurally meaningful. Tiny
    M1 noise is rejected while the original zone coordinates/lifecycle remain
    unchanged.
    """
    active =[]
    if len (candles )<7 :
        return active 

    atr =calculate_atr (candles )or 1.0 
    start =max (2 ,len (candles )-100 )
    cands =[]

    recent_ranges =[max (c ["high"]-c ["low"],0.0 )for c in candles [-30 :]]
    avg_range =sum (recent_ranges )/max (1 ,len (recent_ranges ))

    for i in range (start ,len (candles )):
        left ,mid ,right =candles [i -2 ],candles [i -1 ],candles [i ]
        mid_range =max (mid ["high"]-mid ["low"],0.0 )
        mid_body =abs (mid ["close"]-mid ["open"])
        body_ratio =mid_body /mid_range if mid_range else 0.0 
        range_ratio =mid_range /max (atr ,1e-9 )
        displacement =mid_range /max (avg_range ,1e-9 )

        if right ["low"]>left ["high"]:
            gap =right ["low"]-left ["high"]
            direction ="bullish"
            valid =mid ["close"]>mid ["open"]
        elif right ["high"]<left ["low"]:
            gap =left ["low"]-right ["high"]
            direction ="bearish"
            valid =mid ["close"]<mid ["open"]
        else :
            continue 

            
            
            
            
            
        gap_atr =gap /max (atr ,1e-9 )
        if gap_atr <0.20 :
            continue 
        if range_ratio <1.20 or displacement <1.05 :
            continue 
        if body_ratio <0.60 or not valid :
            continue 

        quality =0.0 
        quality +=min (30.0 ,gap_atr *30.0 )
        quality +=min (25.0 ,range_ratio *15.0 )
        quality +=min (20.0 ,displacement *12.0 )
        quality +=min (15.0 ,body_ratio *15.0 )
        
        quality +=10.0 if mid_body >=atr *0.5 else 0.0 
        quality =round (min (100.0 ,quality ),1 )

        if direction =="bullish":
            top ,bottom =right ["low"],left ["high"]
            color ,border ="rgba(0,255,136,0.16)","#00ff88"
            label ="BULLISH FVG"
        else :
            top ,bottom =left ["low"],right ["high"]
            color ,border ="rgba(255,68,68,0.16)","#ff4444"
            label ="BEARISH FVG"

        if quality <62.0 :
            continue 

        cands .append ({
        "type":"Bullish FVG"if direction =="bullish"else "Bearish FVG",
        "label":label ,"direction":direction ,
        "top":round (top ,2 ),"bottom":round (bottom ,2 ),
        "timeStart":left ["time"],"createdTime":right ["time"],
        "createdIndex":i ,"color":color ,"borderColor":border ,
        "layer":"fvg","status":"active","valid":True ,
        "quality":quality ,"gap":round (gap ,4 ),
        "gap_atr":round (gap_atr ,3 ),
        "displacement_atr":round (range_ratio ,3 ),
        "body_ratio":round (body_ratio ,3 ),
        "displacement":round (displacement ,3 )
        })

    for zone in cands :
        state =_zone_state (zone ,candles )
        if state .get ("status")in {"violated","invalid"}:
            if violated_out is not None and state .get ("status")=="violated":
                v =dict (zone )
                v ["violatedTime"]=state .get ("violated_time")
                v .pop ("createdIndex",None )
                violated_out .append (v )
            continue 
        zone .pop ("createdIndex",None )
        _add_zone_state (zone ,candles )
        active .append (zone )

    active .sort (key =lambda z :(z .get ("quality",0 ),z .get ("createdTime",0 )))
    return active [-max_zones :]


def detect_order_blocks (
candles ,
max_zones =6 ,
violated_out =None 
):
    """
    EXISTING ORDER BLOCK METHODOLOGY PRESERVED.

    OB requires:
      1. The OB candle.
      2. The next candle to break the OB candle.
      3. The next candle to create a valid FVG in the same
         direction.
      4. Existing three-close violation protection.

    No alternate OB methodology is introduced here.
    """

    active =[]

    if len (candles )<10 :
        return active 

    atr =calculate_atr (candles )or 1.0 

    avg_body =sum (
    abs (
    c ["close"]-
    c ["open"]
    )
    for c in candles [-20 :]
    )/min (
    20 ,
    len (candles )
    )

    start =max (
    2 ,
    len (candles )-80 
    )

    cands =[]

    
    
    

    valid_fvgs =[]

    for i in range (
    start ,
    len (candles )
    ):

        if i <2 :
            continue 

        left =candles [i -2 ]
        right =candles [i ]

        if right ["low"]>left ["high"]:

            gap =(
            right ["low"]-
            left ["high"]
            )

            if gap >=atr *0.10 :

                fvg ={
                "direction":"bullish",
                "createdTime":right ["time"],
                "createdIndex":i ,
                "top":right ["low"],
                "bottom":left ["high"]
                }

                if not is_fvg_filled (
                fvg ,
                candles 
                ):
                    valid_fvgs .append (
                    fvg 
                    )

        elif right ["high"]<left ["low"]:

            gap =(
            left ["low"]-
            right ["high"]
            )

            if gap >=atr *0.10 :

                fvg ={
                "direction":"bearish",
                "createdTime":right ["time"],
                "createdIndex":i ,
                "top":left ["low"],
                "bottom":right ["high"]
                }

                if not is_fvg_filled (
                fvg ,
                candles 
                ):
                    valid_fvgs .append (
                    fvg 
                    )

    for i in range (
    start ,
    len (candles )-2 
    ):

        cur =candles [i ]
        nxt =candles [i +1 ]

        if (
        abs (
        nxt ["close"]-
        nxt ["open"]
        )
        <=
        avg_body *1.25 
        or 
        abs (
        nxt ["close"]-
        nxt ["open"]
        )
        <=
        atr *0.35 
        ):
            continue 

            
            
            

        if (
        cur ["close"]<cur ["open"]
        and 
        nxt ["close"]>cur ["high"]
        ):

            following_fvg =any (
            fvg .get ("direction")=="bullish"
            and 
            fvg .get ("createdIndex")==i +1 
            for fvg in valid_fvgs 
            )

            if not following_fvg :
                continue 

            cands .append ({
            "type":"Bullish Order Block",
            "label":"BULLISH OB",
            "direction":"bullish",
            "top":round (
            cur ["high"],
            2 
            ),
            "bottom":round (
            cur ["low"],
            2 
            ),
            "timeStart":cur ["time"],
            "createdTime":nxt ["time"],
            "createdIndex":i +1 ,
            "color":"rgba(34,197,94,0.18)",
            "borderColor":"#22c55e",
            "layer":"ob",
            "status":"active",
            "valid":True 
            })

            
            
            

        elif (
        cur ["close"]>cur ["open"]
        and 
        nxt ["close"]<cur ["low"]
        ):

            following_fvg =any (
            fvg .get ("direction")=="bearish"
            and 
            fvg .get ("createdIndex")==i +1 
            for fvg in valid_fvgs 
            )

            if not following_fvg :
                continue 

            cands .append ({
            "type":"Bearish Order Block",
            "label":"BEARISH OB",
            "direction":"bearish",
            "top":round (
            cur ["high"],
            2 
            ),
            "bottom":round (
            cur ["low"],
            2 
            ),
            "timeStart":cur ["time"],
            "createdTime":nxt ["time"],
            "createdIndex":i +1 ,
            "color":"rgba(239,68,68,0.18)",
            "borderColor":"#ef4444",
            "layer":"ob",
            "status":"active",
            "valid":True 
            })

            
            
            

    for zone in cands :

        _state =_zone_state (
        zone ,
        candles 
        )

        if _state .get ("status")in {"violated","invalid"}:

            if (
            violated_out is not None 
            and _state .get ("status")=="violated"
            ):
                _v =dict (zone )
                _v ["violatedTime"]=_state .get ("violated_time")
                _v .pop ("createdIndex",None )
                violated_out .append (_v )

            continue 

        zone .pop (
        "createdIndex",
        None 
        )

        _add_zone_state (
        zone ,
        candles 
        )

        active .append (zone )

    return active [-max_zones :]


def calculate_pd (candles ):
    lookback =min (
    80 ,
    len (candles )
    )

    recent =candles [-lookback :]

    swing_high =max (
    c ["high"]
    for c in recent 
    )

    swing_low =min (
    c ["low"]
    for c in recent 
    )

    eq =(
    swing_high +
    swing_low 
    )/2 

    zone =(
    "PREMIUM"
    if candles [-1 ]["close"]>eq 
    else "DISCOUNT"
    )

    return {
    "swing_high":round (
    swing_high ,
    2 
    ),
    "swing_low":round (
    swing_low ,
    2 
    ),
    "equilibrium":round (
    eq ,
    2 
    ),
    "current_zone":zone 
    }


    
    
    

def _pattern_tolerance (candles ):
    atr =calculate_atr (candles )or 1.0 
    return max (
    atr *0.35 ,
    safe_float (
    candles [-1 ]["close"],
    0 
    )*0.0008 
    )


def _pattern_swing_points (candles ):
    highs =find_swing_highs (
    candles ,
    2 ,
    2 
    )

    lows =find_swing_lows (
    candles ,
    2 ,
    2 
    )

    points =[]

    for idx in highs :
        points .append ({
        "index":idx ,
        "type":"HIGH",
        "price":candles [idx ]["high"],
        "time":candles [idx ]["time"]
        })

    for idx in lows :
        points .append ({
        "index":idx ,
        "type":"LOW",
        "price":candles [idx ]["low"],
        "time":candles [idx ]["time"]
        })

    points .sort (
    key =lambda x :x ["index"]
    )

    return highs ,lows ,points 


def _pattern_result (
name ,
direction ,
status ,
start_index ,
confirmation_index ,
neckline =None ,
breakout =None ,
reason =None 
):
    return {
    "name":name ,
    "type":"CHART_PATTERN",
    "direction":direction ,
    "status":status ,
    "confirmed":status =="CONFIRMED",
    "valid":status =="CONFIRMED",
    "startIndex":start_index ,
    "confirmationIndex":confirmation_index ,
    "neckline":(
    round (neckline ,2 )
    if neckline is not None 
    else None 
    ),
    "breakout":(
    round (breakout ,2 )
    if breakout is not None 
    else None 
    ),
    "reason":reason or "",
    "layer":"pattern"
    }


def _detect_double_patterns (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (highs )>=2 :

        h1 =highs [-2 ]
        h2 =highs [-1 ]

        p1 =candles [h1 ]["high"]
        p2 =candles [h2 ]["high"]

        if abs (p1 -p2 )<=tolerance :

            between_lows =[
            i 
            for i in lows 
            if h1 <i <h2 
            ]

            if between_lows :

                neckline_idx =min (
                between_lows ,
                key =lambda i :
                candles [i ]["low"]
                )

                neckline =candles [
                neckline_idx 
                ]["low"]

                current =candles [-1 ]

                if current ["close"]<neckline :

                    results .append (
                    _pattern_result (
                    "Double Top",
                    "bearish",
                    "CONFIRMED",
                    h1 ,
                    len (candles )-1 ,
                    neckline ,
                    current ["close"],
                    "Neckline confirmed by closing price below support"
                    )
                    )

    if len (lows )>=2 :

        l1 =lows [-2 ]
        l2 =lows [-1 ]

        p1 =candles [l1 ]["low"]
        p2 =candles [l2 ]["low"]

        if abs (p1 -p2 )<=tolerance :

            between_highs =[
            i 
            for i in highs 
            if l1 <i <l2 
            ]

            if between_highs :

                neckline_idx =max (
                between_highs ,
                key =lambda i :
                candles [i ]["high"]
                )

                neckline =candles [
                neckline_idx 
                ]["high"]

                current =candles [-1 ]

                if current ["close"]>neckline :

                    results .append (
                    _pattern_result (
                    "Double Bottom",
                    "bullish",
                    "CONFIRMED",
                    l1 ,
                    len (candles )-1 ,
                    neckline ,
                    current ["close"],
                    "Neckline confirmed by closing price above resistance"
                    )
                    )

    return results 


def _detect_head_shoulders (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (highs )>=3 :

        left =highs [-3 ]
        head =highs [-2 ]
        right =highs [-1 ]

        left_price =candles [left ]["high"]
        head_price =candles [head ]["high"]
        right_price =candles [right ]["high"]

        shoulder_difference =(
        abs (left_price -right_price )
        <=tolerance *1.5 
        )

        head_higher =(
        head_price >left_price +tolerance 
        and 
        head_price >right_price +tolerance 
        )

        if shoulder_difference and head_higher :

            neckline_lows =[
            i 
            for i in lows 
            if left <i <right 
            ]

            if len (neckline_lows )>=2 :

                neckline =(
                candles [
                neckline_lows [0 ]
                ]["low"]
                +
                candles [
                neckline_lows [-1 ]
                ]["low"]
                )/2 

                current =candles [-1 ]

                if current ["close"]<neckline :

                    results .append (
                    _pattern_result (
                    "Head & Shoulders",
                    "bearish",
                    "CONFIRMED",
                    left ,
                    len (candles )-1 ,
                    neckline ,
                    current ["close"],
                    "Neckline confirmed by closing price below support"
                    )
                    )

    if len (lows )>=3 :

        left =lows [-3 ]
        head =lows [-2 ]
        right =lows [-1 ]

        left_price =candles [left ]["low"]
        head_price =candles [head ]["low"]
        right_price =candles [right ]["low"]

        shoulder_difference =(
        abs (left_price -right_price )
        <=tolerance *1.5 
        )

        head_lower =(
        head_price <left_price -tolerance 
        and 
        head_price <right_price -tolerance 
        )

        if shoulder_difference and head_lower :

            neckline_highs =[
            i 
            for i in highs 
            if left <i <right 
            ]

            if len (neckline_highs )>=2 :

                neckline =(
                candles [
                neckline_highs [0 ]
                ]["high"]
                +
                candles [
                neckline_highs [-1 ]
                ]["high"]
                )/2 

                current =candles [-1 ]

                if current ["close"]>neckline :

                    results .append (
                    _pattern_result (
                    "Inverse Head & Shoulders",
                    "bullish",
                    "CONFIRMED",
                    left ,
                    len (candles )-1 ,
                    neckline ,
                    current ["close"],
                    "Neckline confirmed by closing price above resistance"
                    )
                    )

    return results 


def _detect_triangles (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (highs )<3 or len (lows )<3 :
        return results 

    h1 ,h2 ,h3 =highs [-3 :]
    l1 ,l2 ,l3 =lows [-3 :]

    hp1 =candles [h1 ]["high"]
    hp2 =candles [h2 ]["high"]
    hp3 =candles [h3 ]["high"]

    lp1 =candles [l1 ]["low"]
    lp2 =candles [l2 ]["low"]
    lp3 =candles [l3 ]["low"]

    current =candles [-1 ]

    highs_flat =(
    abs (hp1 -hp2 )<=tolerance *1.5 
    and 
    abs (hp2 -hp3 )<=tolerance *1.5 
    )

    lows_rising =(
    lp2 >lp1 +tolerance *0.20 
    and 
    lp3 >lp2 +tolerance *0.20 
    )

    if highs_flat and lows_rising :

        resistance =(
        hp1 +hp2 +hp3 
        )/3 

        if current ["close"]>resistance :

            results .append (
            _pattern_result (
            "Ascending Triangle",
            "bullish",
            "CONFIRMED",
            h1 ,
            len (candles )-1 ,
            resistance ,
            current ["close"],
            "Resistance breakout confirmed by closing price"
            )
            )

    lows_flat =(
    abs (lp1 -lp2 )<=tolerance *1.5 
    and 
    abs (lp2 -lp3 )<=tolerance *1.5 
    )

    highs_falling =(
    hp2 <hp1 -tolerance *0.20 
    and 
    hp3 <hp2 -tolerance *0.20 
    )

    if lows_flat and highs_falling :

        support =(
        lp1 +lp2 +lp3 
        )/3 

        if current ["close"]<support :

            results .append (
            _pattern_result (
            "Descending Triangle",
            "bearish",
            "CONFIRMED",
            l1 ,
            len (candles )-1 ,
            support ,
            current ["close"],
            "Support breakdown confirmed by closing price"
            )
            )

    highs_falling_sym =(
    hp2 <hp1 -tolerance *0.15 
    and 
    hp3 <hp2 -tolerance *0.15 
    )

    lows_rising_sym =(
    lp2 >lp1 +tolerance *0.15 
    and 
    lp3 >lp2 +tolerance *0.15 
    )

    if highs_falling_sym and lows_rising_sym :

        resistance =hp3 
        support =lp3 

        if current ["close"]>resistance :

            results .append (
            _pattern_result (
            "Symmetrical Triangle",
            "bullish",
            "CONFIRMED",
            min (h1 ,l1 ),
            len (candles )-1 ,
            resistance ,
            current ["close"],
            "Upper triangle boundary breakout confirmed by close"
            )
            )

        elif current ["close"]<support :

            results .append (
            _pattern_result (
            "Symmetrical Triangle",
            "bearish",
            "CONFIRMED",
            min (h1 ,l1 ),
            len (candles )-1 ,
            support ,
            current ["close"],
            "Lower triangle boundary breakdown confirmed by close"
            )
            )

    return results 


def _detect_flags (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (candles )<20 :
        return results 

    recent =candles [-20 :]

    first_close =recent [0 ]["close"]
    last_close =recent [-1 ]["close"]

    move =last_close -first_close 

    current =candles [-1 ]

    recent_highs =[
    i 
    for i in highs 
    if i >=len (candles )-20 
    ]

    recent_lows =[
    i 
    for i in lows 
    if i >=len (candles )-20 
    ]

    if move >tolerance *4 :

        if len (recent_highs )>=2 and len (recent_lows )>=2 :

            h1 =recent_highs [-2 ]
            h2 =recent_highs [-1 ]

            l1 =recent_lows [-2 ]
            l2 =recent_lows [-1 ]

            flag_high_falling =(
            candles [h2 ]["high"]
            <=
            candles [h1 ]["high"]
            +tolerance 
            )

            flag_low_falling =(
            candles [l2 ]["low"]
            <
            candles [l1 ]["low"]
            )

            flag_high =max (
            candles [h1 ]["high"],
            candles [h2 ]["high"]
            )

            if (
            flag_high_falling 
            and 
            flag_low_falling 
            and 
            current ["close"]>flag_high 
            ):

                results .append (
                _pattern_result (
                "Bull Flag",
                "bullish",
                "CONFIRMED",
                len (candles )-20 ,
                len (candles )-1 ,
                flag_high ,
                current ["close"],
                "Flag resistance breakout confirmed by close"
                )
                )

    if move <-tolerance *4 :

        if len (recent_highs )>=2 and len (recent_lows )>=2 :

            h1 =recent_highs [-2 ]
            h2 =recent_highs [-1 ]

            l1 =recent_lows [-2 ]
            l2 =recent_lows [-1 ]

            flag_high_rising =(
            candles [h2 ]["high"]
            >
            candles [h1 ]["high"]
            )

            flag_low_rising =(
            candles [l2 ]["low"]
            >=
            candles [l1 ]["low"]-tolerance 
            )

            flag_low =min (
            candles [l1 ]["low"],
            candles [l2 ]["low"]
            )

            if (
            flag_high_rising 
            and 
            flag_low_rising 
            and 
            current ["close"]<flag_low 
            ):

                results .append (
                _pattern_result (
                "Bear Flag",
                "bearish",
                "CONFIRMED",
                len (candles )-20 ,
                len (candles )-1 ,
                flag_low ,
                current ["close"],
                "Flag support breakdown confirmed by close"
                )
                )

    return results 


def _detect_wedges (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (highs )<3 or len (lows )<3 :
        return results 

    h1 ,h2 ,h3 =highs [-3 :]
    l1 ,l2 ,l3 =lows [-3 :]

    hp1 =candles [h1 ]["high"]
    hp2 =candles [h2 ]["high"]
    hp3 =candles [h3 ]["high"]

    lp1 =candles [l1 ]["low"]
    lp2 =candles [l2 ]["low"]
    lp3 =candles [l3 ]["low"]

    current =candles [-1 ]

    highs_rising =(
    hp2 >hp1 +tolerance *0.15 
    and 
    hp3 >hp2 +tolerance *0.15 
    )

    lows_rising =(
    lp2 >lp1 +tolerance *0.15 
    and 
    lp3 >lp2 +tolerance *0.15 
    )

    high_slope =hp3 -hp1 
    low_slope =lp3 -lp1 

    if (
    highs_rising 
    and 
    lows_rising 
    and 
    low_slope >high_slope 
    ):

        support =lp3 

        if current ["close"]<support :

            results .append (
            _pattern_result (
            "Rising Wedge",
            "bearish",
            "CONFIRMED",
            min (h1 ,l1 ),
            len (candles )-1 ,
            support ,
            current ["close"],
            "Lower wedge boundary breakdown confirmed by close"
            )
            )

    highs_falling =(
    hp2 <hp1 -tolerance *0.15 
    and 
    hp3 <hp2 -tolerance *0.15 
    )

    lows_falling =(
    lp2 <lp1 -tolerance *0.15 
    and 
    lp3 <lp2 -tolerance *0.15 
    )

    high_drop =hp1 -hp3 
    low_drop =lp1 -lp3 

    if (
    highs_falling 
    and 
    lows_falling 
    and 
    high_drop >low_drop 
    ):

        resistance =hp3 

        if current ["close"]>resistance :

            results .append (
            _pattern_result (
            "Falling Wedge",
            "bullish",
            "CONFIRMED",
            min (h1 ,l1 ),
            len (candles )-1 ,
            resistance ,
            current ["close"],
            "Upper wedge boundary breakout confirmed by close"
            )
            )

    return results 


def _detect_range (
candles ,
highs ,
lows ,
tolerance 
):
    results =[]

    if len (candles )<20 :
        return results 

    recent =candles [-20 :]

    range_high =max (
    c ["high"]
    for c in recent 
    )

    range_low =min (
    c ["low"]
    for c in recent 
    )

    range_size =(
    range_high -
    range_low 
    )

    atr =calculate_atr (
    candles 
    )or 1.0 

    if range_size <=atr *8 :

        current =candles [-1 ]

        if current ["close"]>range_high :

            results .append (
            _pattern_result (
            "Range / Consolidation",
            "bullish",
            "CONFIRMED",
            len (candles )-20 ,
            len (candles )-1 ,
            range_high ,
            current ["close"],
            "Range resistance breakout confirmed by close"
            )
            )

        elif current ["close"]<range_low :

            results .append (
            _pattern_result (
            "Range / Consolidation",
            "bearish",
            "CONFIRMED",
            len (candles )-20 ,
            len (candles )-1 ,
            range_low ,
            current ["close"],
            "Range support breakdown confirmed by close"
            )
            )

    return results 


def detect_chart_patterns (candles ):
    if len (candles )<30 :
        return []

    highs ,lows ,_ =_pattern_swing_points (
    candles 
    )

    tolerance =_pattern_tolerance (
    candles 
    )

    patterns =[]

    patterns .extend (
    _detect_double_patterns (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    patterns .extend (
    _detect_head_shoulders (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    patterns .extend (
    _detect_triangles (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    patterns .extend (
    _detect_flags (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    patterns .extend (
    _detect_wedges (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    patterns .extend (
    _detect_range (
    candles ,
    highs ,
    lows ,
    tolerance 
    )
    )

    confirmed =[
    p 
    for p in patterns 
    if p .get ("status")=="CONFIRMED"
    and p .get ("confirmed")is True 
    and p .get ("valid")is True 
    ]

    confirmed .sort (
    key =lambda p :
    p .get (
    "confirmationIndex",
    -1 
    )
    )

    return confirmed [-8 :]


def build_pattern_markers (patterns ,candles ):
    markers =[]

    if not patterns or not candles :
        return markers 

    for pattern in patterns :

        idx =pattern .get (
        "confirmationIndex"
        )

        if idx is None :
            idx =len (candles )-1 

        idx =max (
        0 ,
        min (
        idx ,
        len (candles )-1 
        )
        )

        direction =str (
        pattern .get (
        "direction",
        ""
        )
        ).lower ()

        label =(
        "CONFIRMED "
        +
        pattern .get (
        "name",
        "PATTERN"
        )
        )

        markers .append ({
        "time":candles [idx ]["time"],
        "label":label ,
        "type":"PATTERN",
        "pattern":pattern .get (
        "name"
        ),
        "direction":direction ,
        "confirmed":True 
        })

    return markers 


def get_pattern_confirmation (patterns ):
    bullish =[
    p 
    for p in patterns 
    if p .get ("direction")=="bullish"
    and p .get ("confirmed")is True 
    ]

    bearish =[
    p 
    for p in patterns 
    if p .get ("direction")=="bearish"
    and p .get ("confirmed")is True 
    ]

    if bullish and not bearish :
        return {
        "direction":"BULLISH",
        "count":len (bullish ),
        "patterns":[
        p ["name"]
        for p in bullish 
        ]
        }

    if bearish and not bullish :
        return {
        "direction":"BEARISH",
        "count":len (bearish ),
        "patterns":[
        p ["name"]
        for p in bearish 
        ]
        }

    if bullish and bearish :
        return {
        "direction":"MIXED",
        "count":len (bullish )+len (bearish ),
        "patterns":[
        p ["name"]
        for p in patterns 
        ]
        }

    return {
    "direction":"NONE",
    "count":0 ,
    "patterns":[]
    }


def detect_mss (candles ):
    """Detect MSS as a displacement-based structural shift.

    Bullish MSS: bearish structure (lower high/lower low) is weakening,
    then price displaces and closes above the latest lower high.
    Bearish MSS: bullish structure (higher high/higher low) is weakening,
    then price displaces and closes below the latest higher low.
    """
    result ={"state":"NONE","direction":"NONE","level":None ,"index":None ,"displacement":False }
    if len (candles )<25 :
        return result 

    sh =find_swing_highs (candles ,2 ,2 )
    sl =find_swing_lows (candles ,2 ,2 )
    if len (sh )<2 or len (sl )<2 :
        return result 

    atr =calculate_atr (candles )or 1.0 
    avg_body =sum (abs (c ["close"]-c ["open"])for c in candles [-20 :])/min (20 ,len (candles ))
    displacement_min =max (avg_body *1.05 ,atr *0.20 )

    h1 ,h2 =candles [sh [-2 ]]["high"],candles [sh [-1 ]]["high"]
    l1 ,l2 =candles [sl [-2 ]]["low"],candles [sl [-1 ]]["low"]
    c =candles [-1 ]
    body =abs (c ["close"]-c ["open"])

    
    if h2 <h1 and l2 <l1 :
        protected_high =h2 
        if c ["close"]>protected_high and c ["close"]>c ["open"]and body >=displacement_min :
            return {"state":"BULLISH MSS","direction":"bullish","level":protected_high ,"index":sh [-1 ],"displacement":True }

            
    if h2 >h1 and l2 >l1 :
        protected_low =l2 
        if c ["close"]<protected_low and c ["close"]<c ["open"]and body >=displacement_min :
            return {"state":"BEARISH MSS","direction":"bearish","level":protected_low ,"index":sl [-1 ],"displacement":True }

    return result 

def detect_support_resistance (candles ,max_levels =4 ):
    """Independent S/R engine with confirmed-break and flip lifecycle."""
    if len (candles )<20 :
        return []
    sh =find_swing_highs (candles ,2 ,2 )
    sl =find_swing_lows (candles ,2 ,2 )
    atr =calculate_atr (candles )or 1.0 
    tolerance =max (atr *0.12 ,0.05 )
    levels =[]

    def level_state (price ,kind ,idx ):
        status ="TESTED"
        broken_at =None 
        potential =None 
        retested =False 
        rejected =False 
        for j in range (idx +1 ,len (candles )):
            c =candles [j ]
            close =c ["close"]
            touched =c ["low"]<=price +tolerance and c ["high"]>=price -tolerance 
            if touched :
                status ="TESTED"
            if kind =="RESISTANCE":
                if close >price :
                    if potential is None :
                        potential =j 
                    elif j ==potential +1 :
                        broken_at =j 
                        status ="BROKEN"
                        potential =None 
                elif potential is not None and j ==potential +1 and close <=price :
                    potential =None 
                    status ="TESTED"
                if broken_at is not None and j >broken_at :
                    if c ["low"]<=price +tolerance and close >=price :
                        retested =True 
                        rejected =close >=price 
                        if rejected :
                            status ="CONFIRMED"
            else :
                if close <price :
                    if potential is None :
                        potential =j 
                    elif j ==potential +1 :
                        broken_at =j 
                        status ="BROKEN"
                        potential =None 
                elif potential is not None and j ==potential +1 and close >=price :
                    potential =None 
                    status ="TESTED"
                if broken_at is not None and j >broken_at :
                    if c ["high"]>=price -tolerance and close <=price :
                        retested =True 
                        rejected =close <=price 
                        if rejected :
                            status ="CONFIRMED"
        return status ,broken_at ,retested ,rejected 

    for idx in sh [-max_levels :]:
        p =round (candles [idx ]["high"],2 )
        status ,broken_at ,retested ,rejected =level_state (p ,"RESISTANCE",idx )
        if status in {"BROKEN","CONFIRMED"}:
            direction ="support"
            label ="FLIPPED SUPPORT"if status =="CONFIRMED"else "RESISTANCE BROKEN"
            strategy_direction ="bullish"if status =="CONFIRMED"else "bullish"
        else :
            direction ="resistance"
            label ="RESISTANCE"
            strategy_direction ="bearish"
        levels .append ({"type":"Resistance","label":label ,"direction":strategy_direction ,"price":p ,
        "kind":direction ,"status":status ,"broken":broken_at is not None ,
        "retested":retested ,"respected":rejected ,"index":idx ,
        "lineStyle":0 ,"layer":"structure"})

    for idx in sl [-max_levels :]:
        p =round (candles [idx ]["low"],2 )
        status ,broken_at ,retested ,rejected =level_state (p ,"SUPPORT",idx )
        if status in {"BROKEN","CONFIRMED"}:
            direction ="resistance"
            label ="FLIPPED RESISTANCE"if status =="CONFIRMED"else "SUPPORT BROKEN"
            strategy_direction ="bearish"
        else :
            direction ="support"
            label ="SUPPORT"
            strategy_direction ="bullish"
        levels .append ({"type":"Support","label":label ,"direction":strategy_direction ,"price":p ,
        "kind":direction ,"status":status ,"broken":broken_at is not None ,
        "retested":retested ,"respected":rejected ,"index":idx ,
        "lineStyle":0 ,"layer":"structure"})

        
    levels .sort (key =lambda x :x ["index"],reverse =True )
    out =[]
    for level in levels :
        if any (abs (level ["price"]-x ["price"])<=tolerance for x in out ):
            continue 
        out .append (level )
        if len (out )>=max_levels :
            break 
    return out 


    
NEAR_ZONE_ATR_MULT =2.0 


def _find_near_zone_setup (price ,atr ,zones ):
    """Find the closest untouched, still-valid zone that price is approaching.

    Bullish zone (demand): price above the zone top, coming down to it -> BUY.
    Bearish zone (supply): price below the zone bottom, rising to it -> SELL.
    Returns None when nothing is close enough.
    """
    max_dist =max (atr *NEAR_ZONE_ATR_MULT ,0.10 )
    best =None 

    for z in zones :
        if z .get ("status")!="valid"or z .get ("retested"):
            continue 

        top =safe_float (z .get ("top"),None )
        bottom =safe_float (z .get ("bottom"),None )
        if top is None or bottom is None :
            continue 

        d =str (z .get ("direction","")).lower ()

        if d =="bullish"and price >top :
            dist =price -top 
            direction ="BUY"
        elif d =="bearish"and price <bottom :
            dist =bottom -price 
            direction ="SELL"
        else :
            continue 

        if dist >max_dist :
            continue 

        if best is None or dist <best ["distance"]:
            best ={
            "direction":direction ,
            "distance":round (dist ,2 ),
            "zone":z .get ("label")or z .get ("type"),
            "top":top ,
            "bottom":bottom ,
            "zone_direction":d 
            }

    return best 


def build_smc_commentary (structure ,mss ,liq_events ,fvg_zones ,ob_zones ,sr_levels ,signal_data ,violated_zones =None ):
    comments =[]
    confirmed =[]
    waits =[]

    for kind ,zone_list in (("FVG",fvg_zones ),("OB",ob_zones )):
        for z in zone_list :
            d =z .get ("direction")
            name =("bullish "if d =="bullish"else "bearish ")+kind 
            side ="BUY"if d =="bullish"else "SELL"
            if z .get ("status")=="respected"and z .get ("confirmed"):
                confirmed .append (name )
                if z .get ("swept"):
                    comments .append (f"WICK SWEEP — price wicked through the {name .upper ()} but the candle closed back on the valid side. Zone remains valid; {side } setup confirmed.")
                else :
                    comments .append (f"{name .upper ()} CONFIRMED — price closed inside/respected the zone. {side } setup is valid and the signal is issued immediately.")
            elif z .get ("status")=="potential_violation":
                waits .append (name )
                comments .append (f"WAIT — a candle closed beyond the {name .upper ()}. This is only a first close; wait for the next candle. If it also closes beyond, the zone is violated; if it recovers, the zone stays valid.")
            elif z .get ("retested"):
                waits .append (name )
                comments .append (f"WAIT — {name .upper ()} has been retested. Wait for a close inside/on the valid side of the zone.")
            else :
                waits .append (name )
                comments .append (f"Wait for price to reach the {name } and close inside it before considering a {side } setup.")

    for z in (violated_zones or []):
        d =z .get ("direction")
        name =("bullish "if d =="bullish"else "bearish ")+("OB"if z .get ("layer")=="ob"else "FVG")
        comments .append (f"ZONE VIOLATED — two consecutive closes beyond the {name .upper ()}. The zone is invalid and no longer supports a {'BUY'if d =='bullish'else 'SELL'} setup.")

    for s in sr_levels :
        if s .get ("status")=="CONFIRMED":
            confirmed .append (s ["label"])
            comments .append (f"{s ['label'].upper ()} CONFIRMED — the break and retest respected the flipped level.")
        elif s .get ("status")=="BROKEN":
            waits .append (s ["label"])
            comments .append (f"WAIT — {s ['label'].upper ()} has a first break. Wait for the second candle to confirm the break, then watch the retest.")
        else :
            comments .append (f"{s ['label']} is {s .get ('status','TESTED')}. A wick through the level alone does not flip it.")

    if mss .get ("direction")=="bullish":
        confirmed .append ("bullish MSS")
        comments .append ("STRUCTURE SHIFT — BULLISH MSS detected with displacement through the structural level.")
    elif mss .get ("direction")=="bearish":
        confirmed .append ("bearish MSS")
        comments .append ("STRUCTURE SHIFT — BEARISH MSS detected with displacement through the structural level.")
    else :
        comments .append ("MSS: no qualifying displacement-based market structure shift is currently detected.")

    if structure .get ("event"):
        comments .append (f"Structure event: {structure ['event']} at {structure .get ('level')}.")
    else :
        comments .append (f"Structure state: {structure .get ('state','NEUTRAL')}.")

    for ev in liq_events :
        if ev .get ("type")=="SELL_SIDE_SWEEP":
            comments .append ("LIQUIDITY SWEPT — SELL-SIDE liquidity has been swept. Wait for bullish structure confirmation and a valid FVG/OB or S/R retest.")
        elif ev .get ("type")=="BUY_SIDE_SWEEP":
            comments .append ("LIQUIDITY SWEPT — BUY-SIDE liquidity has been swept. Wait for bearish structure confirmation and a valid FVG/OB or S/R retest.")

    if confirmed :
        comments .append ("CONFLUENCE: "+", ".join (confirmed )+". Respect/confirmation quality is weighted ahead of raw setup count.")
    else :
        comments .append ("CONFLUENCE: no independently confirmed SMC setup is currently present; WAIT for the relevant setup to be respected/confirmed.")

    sig_now =signal_data .get ("signal")
    if sig_now in {"BUY","STRONG BUY"}:
        comments .append (f"{sig_now } SETUP CONFIRMED. Independent bullish SMC evidence is currently respected/confirmed.")
    elif sig_now in {"SELL","STRONG SELL"}:
        comments .append (f"{sig_now } SETUP CONFIRMED. Independent bearish SMC evidence is currently respected/confirmed.")
    else :
        comments .append ("WAIT — no independently confirmed directional SMC setup currently meets the confirmation requirements.")

    return comments 


def build_smc_statistics (interval ,structure ,mss ,liq_events ,fvg_zones ,ob_zones ,pd ,mtf ,trendlines ,signal_data ):
    """Build timeframe-local SMC evidence boxes and a transparent confidence.

    Every layer is derived from the selected timeframe. MTF is kept as one
    separate evidence layer and never replaces the selected timeframe's
    structure, zones, liquidity or trendline analysis.
    """
    quality =signal_data .get ("quality",{})if isinstance (signal_data ,dict )else {}
    bias =quality .get ("direction")
    if bias not in {"BUY","SELL"}:
        if structure .get ("state")=="BULLISH":
            bias ="BUY"
        elif structure .get ("state")=="BEARISH":
            bias ="SELL"
        else :
            bias ="WAIT"

    layers =[]

    def add (key ,title ,weight ,state ,commentary ):
        layers .append ({
        "key":key ,"title":title ,"weight":weight ,
        "state":state ,"commentary":commentary ,
        "confirmed":state =="positive"
        })

        
    if structure .get ("event"):
        bullish =structure .get ("state")=="BULLISH"
        state ="positive"if ((bullish and bias =="BUY")or ((not bullish )and bias =="SELL"))else "negative"
        add ("structure","Market Structure",20 ,state ,
        f"{structure .get ('state','NEUTRAL').title ()} {structure .get ('event')} confirmed on {interval .upper ()}")
    elif mss .get ("direction")in {"bullish","bearish"}:
        bullish =mss .get ("direction")=="bullish"
        state ="positive"if ((bullish and bias =="BUY")or ((not bullish )and bias =="SELL"))else "negative"
        add ("structure","Market Structure",20 ,state ,
        f"{mss .get ('state','MSS')} confirmed on {interval .upper ()}")
    else :
        add ("structure","Market Structure",20 ,"wait",f"Waiting for confirmed {interval .upper ()} BOS/CHoCH/MSS")

        
    bull_liq =any (e .get ("type")=="SELL_SIDE_SWEEP"for e in liq_events )
    bear_liq =any (e .get ("type")=="BUY_SIDE_SWEEP"for e in liq_events )
    if bull_liq or bear_liq :
        liq_bull =bull_liq and not bear_liq 
        positive =(liq_bull and bias =="BUY")or ((not liq_bull )and bias =="SELL")
        add ("liquidity","Liquidity",15 ,"positive"if positive else "negative",
        ("Sell-side liquidity swept"if liq_bull else "Buy-side liquidity swept")+f" on {interval .upper ()}")
    else :
        add ("liquidity","Liquidity",15 ,"wait",f"Waiting for a confirmed {interval .upper ()} liquidity sweep")

        
    bull_fvg =any (z .get ("direction")=="bullish"and z .get ("status")=="respected"and z .get ("confirmed")for z in fvg_zones )
    bear_fvg =any (z .get ("direction")=="bearish"and z .get ("status")=="respected"and z .get ("confirmed")for z in fvg_zones )
    if bull_fvg or bear_fvg :
        fvg_bull =bull_fvg and not bear_fvg 
        positive =(fvg_bull and bias =="BUY")or ((not fvg_bull )and bias =="SELL")
        add ("fvg","Fair Value Gap",12 ,"positive"if positive else "negative",
        ("Bullish FVG confirmed"if fvg_bull else "Bearish FVG confirmed")+f" on {interval .upper ()}")
    else :
        add ("fvg","Fair Value Gap",12 ,"wait",f"Waiting for a significant {interval .upper ()} FVG")

        
    bull_ob =any (z .get ("direction")=="bullish"and z .get ("status")=="respected"and z .get ("confirmed")for z in ob_zones )
    bear_ob =any (z .get ("direction")=="bearish"and z .get ("status")=="respected"and z .get ("confirmed")for z in ob_zones )
    if bull_ob or bear_ob :
        ob_bull =bull_ob and not bear_ob 
        positive =(ob_bull and bias =="BUY")or ((not ob_bull )and bias =="SELL")
        add ("ob","Order Block",12 ,"positive"if positive else "negative",
        ("Bullish OB confirmed"if ob_bull else "Bearish OB confirmed")+f" on {interval .upper ()}")
    else :
        add ("ob","Order Block",12 ,"wait",f"Waiting for a confirmed {interval .upper ()} order block")

        
    if mss .get ("displacement"):
        disp_bull =mss .get ("direction")=="bullish"
        positive =(disp_bull and bias =="BUY")or ((not disp_bull )and bias =="SELL")
        add ("displacement","Displacement",15 ,"positive"if positive else "negative",
        ("Strong bullish displacement confirmed"if disp_bull else "Strong bearish displacement confirmed")+f" on {interval .upper ()}")
    else :
        add ("displacement","Displacement",15 ,"wait",f"Waiting for meaningful {interval .upper ()} displacement")

        
    zone =str (pd .get ("current_zone",pd .get ("zone",""))).upper ()if isinstance (pd ,dict )else ""
    if zone in {"DISCOUNT","PREMIUM"}:
        pd_bull =zone =="DISCOUNT"
        positive =(pd_bull and bias =="BUY")or ((not pd_bull )and bias =="SELL")
        add ("premium_discount","Premium / Discount",10 ,"positive"if positive else "negative",
        f"Price in {zone .title ()} on {interval .upper ()}")
    else :
        add ("premium_discount","Premium / Discount",10 ,"wait",f"Waiting for a clear {interval .upper ()} premium/discount state")

        
    tl_bull =any (t .get ("direction")=="bullish"for t in trendlines )
    tl_bear =any (t .get ("direction")=="bearish"for t in trendlines )
    sweeps =quality .get ("trendline_sweeps",[])
    if sweeps :
        tl_bull =any ("BULLISH"in str (x )for x in sweeps )
        tl_bear =any ("BEARISH"in str (x )for x in sweeps )
    if tl_bull or tl_bear :
        tl_direction ="bullish"if tl_bull and not tl_bear else "bearish"if tl_bear and not tl_bull else None 
        if tl_direction :
            positive =(tl_direction =="bullish"and bias =="BUY")or (tl_direction =="bearish"and bias =="SELL")
            text_value =("Bullish trendline liquidity active"if tl_direction =="bullish"else "Bearish trendline liquidity active")
            if sweeps :
                text_value =("Bullish trendline sweep confirmed"if tl_direction =="bullish"else "Bearish trendline sweep confirmed")
            add ("trendline","Trendline",6 ,"positive"if positive else "negative",text_value +f" on {interval .upper ()}")
        else :
            add ("trendline","Trendline",6 ,"wait",f"{interval .upper ()} trendline evidence is mixed")
    else :
        add ("trendline","Trendline",6 ,"wait",f"No confirmed {interval .upper ()} trendline evidence")

        
    bull_mtf =sum (1 for v in (mtf or {}).values ()if v =="BULLISH")
    bear_mtf =sum (1 for v in (mtf or {}).values ()if v =="BEARISH")
    if bull_mtf >=3 or bear_mtf >=3 :
        mtf_bull =bull_mtf >=bear_mtf 
        positive =(mtf_bull and bias =="BUY")or ((not mtf_bull )and bias =="SELL")
        add ("mtf","MTF Alignment",10 ,"positive"if positive else "negative",
        f"{bull_mtf } bullish / {bear_mtf } bearish timeframe readings")
    else :
        add ("mtf","MTF Alignment",10 ,"wait",f"MTF evidence is mixed: {bull_mtf } bullish / {bear_mtf } bearish")

    if bias in {"BUY","SELL"}:
        positive_weight =sum (x ["weight"]for x in layers if x ["state"]=="positive")
        negative_weight =sum (x ["weight"]for x in layers if x ["state"]=="negative")
        confidence =round (clamp (50 +(positive_weight -negative_weight )*0.5 ,5 ,95 ),1 )
    else :
        confidence =50.0 

    confirmed =sum (1 for x in layers if x ["confirmed"])
    return {
    "timeframe":interval ,
    "bias":bias ,
    "confidence":confidence ,
    "confirmed_setups":confirmed ,
    "total_layers":len (layers ),
    "layers":layers 
    }


def detect_meaningful_trendlines (candles ,max_lines =4 ):
    """Detect only meaningful trendlines for the CURRENT timeframe.

    Trendlines are timeframe-local evidence. They are never copied from
    another timeframe and never generate BUY/SELL by themselves. A line is
    removed as soon as a confirmed close decisively breaks it.
    """
    if len (candles )<35 :
        return []

    sh =find_swing_highs (candles ,3 ,3 )
    sl =find_swing_lows (candles ,3 ,3 )
    lines =[]
    atr_now =calculate_atr (candles )or 1.0 

    def build (points ,bullish ):
        out =[]
        for a_pos in range (max (0 ,len (points )-8 ),len (points )-1 ):
            for b_pos in range (a_pos +1 ,len (points )):
                a ,b =points [a_pos ],points [b_pos ]
                if b -a <8 :
                    continue 

                p1 =candles [a ]["low"if bullish else "high"]
                p2 =candles [b ]["low"if bullish else "high"]
                slope =(p2 -p1 )/float (b -a )

                if bullish and slope <=0 :
                    continue 
                if not bullish and slope >=0 :
                    continue 

                tol =(calculate_atr (candles [max (0 ,b -30 ):b +1 ])or atr_now or 1.0 )*0.18 
                touches =0 
                broken =False 

                for j in range (b +1 ,len (candles )):
                    projected =p2 +slope *(j -b )
                    c =candles [j ]
                    actual =c ["low"if bullish else "high"]

                    if abs (actual -projected )<=tol :
                        touches +=1 

                        
                        
                    if bullish and c ["close"]<projected -tol :
                        broken =True 
                        break 
                    if not bullish and c ["close"]>projected +tol :
                        broken =True 
                        break 

                if broken or touches <1 :
                    continue 

                score =50 +min (25 ,touches *10 )+min (20 ,(b -a )/4 )
                out .append ({
                "type":"BULLISH_TRENDLINE"if bullish else "BEARISH_TRENDLINE",
                "direction":"bullish"if bullish else "bearish",
                "time1":candles [a ]["time"],"price1":round (p1 ,2 ),
                "time2":candles [b ]["time"],"price2":round (p2 ,2 ),
                "slope":slope ,"touches":touches +2 ,
                "quality":round (min (100 ,score ),1 ),
                "liquidity_side":"sell-side"if bullish else "buy-side",
                "layer":"trendline",
                "valid":True ,
                "timeframe":None 
                })
        return out 

    lines .extend (build (sl ,True ))
    lines .extend (build (sh ,False ))
    lines .sort (key =lambda x :(x ["quality"],x ["time2"]),reverse =True )

    chosen =[]
    seen =set ()
    for line in lines :
        key =(line ["direction"],line ["time1"],line ["time2"])
        if key in seen :
            continue 
        seen .add (key )
        chosen .append (line )
        if len (chosen )>=max_lines :
            break 
    return chosen 


def _trendline_context (candles ,trendlines ,atr ):
    price =candles [-1 ]["close"]
    bullish =bearish =0 
    sweep_events =[]
    for tl in trendlines :
        t2 =int (tl ["time2"])
        
        i2 =next ((i for i ,c in enumerate (candles )if int (c ["time"])==t2 ),None )
        if i2 is None :
            continue 
        slope =tl ["slope"]
        projected =tl ["price2"]+slope *(len (candles )-1 -i2 )
        distance =abs (price -projected )
        if distance <=atr *0.35 :
            if tl ["direction"]=="bullish":bullish +=1 
            else :bearish +=1 
        last =candles [-1 ]
        prev =candles [-2 ]if len (candles )>1 else last 
        if tl ["direction"]=="bullish"and prev ["low"]>=projected and last ["low"]<projected and last ["close"]>projected :
            sweep_events .append ("BULLISH TRENDLINE SWEEP")
        elif tl ["direction"]=="bearish"and prev ["high"]<=projected and last ["high"]>projected and last ["close"]<projected :
            sweep_events .append ("BEARISH TRENDLINE SWEEP")
    return bullish ,bearish ,sweep_events 


def _smc_quality_layer (candles ,interval ,mtf ,structure ,liq_events ,fvg_zones ,ob_zones ,pd ,mss ,sr_levels ,trendlines ):
    """Final evidence layer: quality over raw setup count."""
    price =candles [-1 ]["close"]
    atr =calculate_atr (candles )or max (price *0.001 ,0.1 )
    bull =0.0 ;bear =0.0 ;bull_reasons =[];bear_reasons =[]

    def add (side ,points ,reason ):
        nonlocal bull ,bear 
        if side =="BUY":bull +=points ;bull_reasons .append (reason )
        else :bear +=points ;bear_reasons .append (reason )

    if structure .get ("event")in {"BOS","CHoCH"}:
        add ("BUY"if structure .get ("state")=="BULLISH"else "SELL",2.0 ,"confirmed structure break")
    if mss .get ("direction")in {"bullish","bearish"}:
        add ("BUY"if mss ["direction"]=="bullish"else "SELL",2.0 ,"displacement MSS")
    for ev in liq_events :
        if ev .get ("type")=="SELL_SIDE_SWEEP":add ("BUY",2.0 ,"sell-side liquidity sweep")
        if ev .get ("type")=="BUY_SIDE_SWEEP":add ("SELL",2.0 ,"buy-side liquidity sweep")

    for z in fvg_zones +ob_zones :
        if z .get ("status")!="respected"or not z .get ("confirmed"):continue 
        q =float (z .get ("quality",70 ))if z .get ("layer")=="fvg"else 65.0 
        pts =1.0 if q <75 else 1.5 if q <88 else 2.0 
        add ("BUY"if z .get ("direction")=="bullish"else "SELL",pts ,f"quality {z .get ('label','zone')}")

    bull_mtf =sum (1 for v in mtf .values ()if v =="BULLISH")
    bear_mtf =sum (1 for v in mtf .values ()if v =="BEARISH")
    if bull_mtf >=3 :add ("BUY",1.5 ,"MTF bullish agreement")
    if bear_mtf >=3 :add ("SELL",1.5 ,"MTF bearish agreement")
    if bull_mtf <=1 and bear_mtf >=4 :bull -=1.5 
    if bear_mtf <=1 and bull_mtf >=4 :bear -=1.5 

    pd_state =str (pd .get ("zone",pd .get ("state",""))).upper ()if isinstance (pd ,dict )else ""
    if "DISCOUNT"in pd_state :add ("BUY",1.0 ,"discount zone")
    if "PREMIUM"in pd_state :add ("SELL",1.0 ,"premium zone")

    tb ,tr ,sweeps =_trendline_context (candles ,trendlines ,atr )
    if tb :add ("BUY",0.5 ,"bullish trendline liquidity")
    if tr :add ("SELL",0.5 ,"bearish trendline liquidity")
    for ev in sweeps :
        add ("BUY"if "BULLISH"in ev else "SELL",1.0 ,ev .lower ())

    conflict =abs (bull -bear )<1.5 
    direction ="WAIT"
    score =max (bull ,bear )
    if not conflict :
        if bull >bear :direction ="BUY"
        elif bear >bull :direction ="SELL"
        
    if score <5.0 :direction ="WAIT"
    return {"direction":direction ,"bull":round (bull ,2 ),"bear":round (bear ,2 ),"score":round (score ,2 ),"conflict":conflict ,
    "bull_reasons":bull_reasons ,"bear_reasons":bear_reasons ,"trendline_sweeps":sweeps }


def build_signal (
candles ,interval ,mtf ,structure ,liq_events ,fvg_zones ,ob_zones ,pd ,
chart_patterns =None ,mss =None ,sr_levels =None ,trendlines =None 
):
    price =candles [-1 ]["close"]
    rsi =calculate_rsi ([c ["close"]for c in candles ])
    reasons =[];mss =mss or {"direction":"NONE"};sr_levels =sr_levels or [];chart_patterns =chart_patterns or [];trendlines =trendlines or []
    quality =_smc_quality_layer (candles ,interval ,mtf ,structure ,liq_events ,fvg_zones ,ob_zones ,pd ,mss ,sr_levels ,trendlines )
    direction =quality ["direction"]
    reasons .extend (quality ["bull_reasons"]if direction =="BUY"else quality ["bear_reasons"]if direction =="SELL"else quality ["bull_reasons"]+quality ["bear_reasons"])
    if quality ["conflict"]:reasons .append ("MTF/SMC evidence conflict — WAIT")
    if direction =="WAIT":reasons .append ("WAIT — no sufficiently confirmed, high-quality SMC setup")

    strong =quality ["score"]>=7.0 and not quality ["conflict"]
    sig ="STRONG BUY"if direction =="BUY"and strong else "BUY"if direction =="BUY"else "STRONG SELL"if direction =="SELL"and strong else "SELL"if direction =="SELL"else "WAIT"
    score =quality ["score"]if direction =="BUY"else -quality ["score"]if direction =="SELL"else 0 
    confidence =round (clamp (50 +min (45 ,quality ["score"]*6 ),50 ,95 ))if direction !="WAIT"else 50 
    atr =calculate_atr (candles )or max (price *0.001 ,0.1 )
    
    return {"signal":sig ,"score":round (score ,2 ),"confidence":confidence ,"rsi":rsi ,"reasons":reasons ,
    "bullish_mtf":sum (1 for v in mtf .values ()if v =="BULLISH"),
    "bearish_mtf":sum (1 for v in mtf .values ()if v =="BEARISH"),"atr":round (atr ,4 ),
    "confirmed_patterns":[p ["name"]for p in chart_patterns ],"bullish_setups":quality ["bull_reasons"],
    "bearish_setups":quality ["bear_reasons"],"early":False ,"early_zone":None ,"quality":quality }


def smc_analysis (interval ="5m"):
    interval =normalize_tf (
    interval 
    )

    
    
    

    live_candles =[
    dict (c )
    for c in get_gold_with_live (interval )
    ]

    
    
    
    static_candles =[
    dict (c )
    for c in live_candles 
    ]

    smc_candles =live_candles 

    if len (smc_candles )<40 :

        historical =_smc_get_historical_signals (
        interval 
        )

        return {
        "status":"INSUFFICIENT_DATA",
        "symbol":SYMBOL ,
        "timeframe":interval ,
        "price":(
        live_candles [-1 ]["close"]
        if live_candles 
        else 0 
        ),
        "signal":"WAIT",
        "score":0 ,
        "confidence":0 ,
        "rsi":50 ,
        "candles":live_candles ,

        "markers":[
        {
        "time":item ["time"],
        "label":item ["signal"],
        "type":"SIGNAL",
        "signal":item ["signal"],
        "direction":item ["direction"],
        "price":item ["price"],
        "size":1 ,
        "fontSize":8 ,
        "immediate":True ,
        "historical":True 
        }
        for item 
        in historical ["signals"]
        ],

        "lines":[],
        "zones":[],
        "patterns":[],

        "pattern_confirmation":{
        "direction":"NONE",
        "count":0 ,
        "patterns":[]
        },

        "reasons":[
        "Insufficient history"
        ],

        "mtf_matrix":{},
        "pd_zones":{},

        "signal_history":historical ["signals"],

        "fixed_entries":historical [
        "entries"
        ],

        "active_entry":historical [
        "active_entry"
        ],

        "last_signal_direction":historical [
        "last_direction"
        ]
        }

    price =(
    live_candles [-1 ]["close"]
    if live_candles 
    else static_candles [-1 ]["close"]
    )

    mtf =build_mtf_matrix ()

    
    structure =detect_structure (
    smc_candles 
    )

    liq_lines ,liq_events =detect_liquidity (
    smc_candles 
    )

    violated_zones =[]

    fvg_zones =detect_fvgs (
    smc_candles ,
    violated_out =violated_zones 
    )

    ob_zones =detect_order_blocks (
    smc_candles ,
    violated_out =violated_zones 
    )

    mss =detect_mss (
    smc_candles 
    )

    trendlines =detect_meaningful_trendlines (smc_candles )
    for _tl in trendlines :
        _tl ["timeframe"]=interval 

    sr_levels =detect_support_resistance (
    smc_candles 
    )

    pd =calculate_pd (
    smc_candles 
    )

    chart_patterns =detect_chart_patterns (
    smc_candles 
    )

    pattern_confirmation =get_pattern_confirmation (
    chart_patterns 
    )

    signal_data =build_signal (
    smc_candles ,
    interval ,
    mtf ,
    structure ,
    liq_events ,
    fvg_zones ,
    ob_zones ,
    pd ,
    chart_patterns ,
    mss ,
    sr_levels ,
    trendlines 
    )

    lines =[]

    for tl in trendlines :
        lines .append (dict (tl ))

    if structure ["level"]is not None :

        lines .append ({
        "price":structure ["level"],
        "color":(
        "#00ff88"
        if structure ["state"]=="BULLISH"
        else "#ff4444"
        ),
        "title":(
        structure ["event"]
        or 
        structure ["state"]
        ),
        "lineStyle":0 ,
        "layer":"structure",
        "active":True 
        })

    lines .extend (
    liq_lines 
    )

    for sr in sr_levels :
        lines .append ({
        "price":sr ["price"],
        "color":"#00ff88"if sr ["direction"]=="bullish"else "#ff4444",
        "title":sr ["label"],
        "lineStyle":0 ,
        "layer":"structure",
        "active":True 
        })

    zones =[]

    zones .extend (
    fvg_zones 
    )

    zones .extend (
    ob_zones 
    )

    markers =[]

    
    if structure ["event"]:

        markers .append ({
        "time":smc_candles [-1 ]["time"],
        "label":structure ["event"],
        "type":"STRUCTURE"
        })

        
    for ev in liq_events :

        markers .append ({
        "time":ev ["time"],
        "label":ev ["label"],
        "type":"LIQUIDITY"
        })

    if mss .get ("direction")!="NONE":
        markers .append ({
        "time":smc_candles [-1 ]["time"],
        "label":mss ["state"],
        "type":"MSS",
        "direction":mss ["direction"],
        "price":mss .get ("level")
        })

    for sr in sr_levels :
        if sr .get ("status")in {"BROKEN","CONFIRMED"}:
            markers .append ({
            "time":smc_candles [-1 ]["time"],
            "label":sr ["label"],
            "type":"SUPPORT_RESISTANCE",
            "direction":sr .get ("direction"),
            "price":sr .get ("price")
            })

            
    pattern_markers =build_pattern_markers (
    chart_patterns ,
    smc_candles 
    )

    markers .extend (
    pattern_markers 
    )

    
    
    
    
    
    
    
    
    
    
    
    
    

    signal =signal_data ["signal"]

    if signal in {
    "BUY",
    "STRONG BUY",
    "SELL",
    "STRONG SELL"
    }:

        signal_candle =(
        live_candles [-1 ]
        if live_candles 
        else static_candles [-1 ]
        )

        q =signal_data .get ("quality",{})
        
        
        
        
        setup_parts =[
        str (signal .split ()[-1 ]),
        str (q .get ("direction")),
        str (structure .get ("event")),
        str (structure .get ("index")),
        str (mss .get ("direction")),
        str (mss .get ("index"))
        ]
        setup_parts .extend (
        sorted (
        f"{ev .get ('type')}:{ev .get ('time')}"
        for ev in liq_events 
        )
        )
        setup_parts .extend (
        sorted (
        f"{z .get ('layer')}:{z .get ('direction')}:{z .get ('createdTime')}"
        for z in (fvg_zones +ob_zones )
        if z .get ("status")in {"respected","valid","active"}
        )
        )
        setup_parts .extend (
        sorted (
        f"trendline:{tl .get ('direction')}:{tl .get ('time1')}:{tl .get ('time2')}"
        for tl in trendlines 
        )
        )
        setup_id ="|".join (setup_parts )

        if "BUY"in signal :
            _smc_record_signal (interval ,"BUY",signal ,signal_candle ,setup_id )
        elif "SELL"in signal :
            _smc_record_signal (interval ,"SELL",signal ,signal_candle ,setup_id )

            
    _recent_cut =smc_candles [-10 ]["time"]

    recent_violated =sorted (
    [
    z for z in violated_zones 
    if (z .get ("violatedTime")or 0 )>=_recent_cut 
    ],
    key =lambda z :z .get ("violatedTime")or 0 
    )[-2 :]

    commentary =build_smc_commentary (
    structure ,
    mss ,
    liq_events ,
    fvg_zones ,
    ob_zones ,
    sr_levels ,
    signal_data ,
    recent_violated 
    )

    smc_statistics =build_smc_statistics (
    interval ,
    structure ,
    mss ,
    liq_events ,
    fvg_zones ,
    ob_zones ,
    pd ,
    mtf ,
    trendlines ,
    signal_data 
    )
    signal_data ["confidence"]=smc_statistics ["confidence"]

    
    
    
    
    
    
    
    
    

    historical =_smc_get_historical_signals (
    interval 
    )

    
    
    
    
    
    
    

    _active =historical ["active_entry"]

    if _active :
        fixed_signal =_active ["signal"]
        commentary .append (
        f"SIGNAL LOCKED — {_active ['signal']} entry fixed at {_active ['price']}. "
        f"It will not move with price and stays fixed until an opposite signal is confirmed."
        )
    else :
        fixed_signal =signal_data ["signal"]

    historical_times ={
    (
    marker .get ("time"),
    marker .get ("direction")
    )
    for marker in markers 
    if marker .get ("type")=="SIGNAL"
    }

    for item in historical ["signals"]:

        key =(
        item ["time"],
        item ["direction"]
        )

        if key in historical_times :
            continue 

        markers .append ({
        "time":item ["time"],
        "label":item ["signal"],
        "type":"SIGNAL",
        "signal":item ["signal"],
        "direction":item ["direction"],

        
        "price":item ["price"],

        "size":1 ,
        "fontSize":8 ,
        "immediate":True ,
        "historical":True 
        })

    return {
    "status":"OK",
    "symbol":SYMBOL ,
    "timeframe":interval ,

    "price":price ,

    
    "signal":fixed_signal ,
    "live_signal":signal_data ["signal"],
    "signal_locked":bool (_active ),
    "entry_price":(
    _active ["price"]
    if _active 
    else None 
    ),
    "entry_time":(
    _active ["time"]
    if _active 
    else None 
    ),
    "early_signal":False ,

    "score":signal_data ["score"],
    "confidence":signal_data ["confidence"],
    "rsi":signal_data ["rsi"],

    "candles":(
    live_candles 
    if live_candles 
    else static_candles 
    ),

    "markers":markers ,
    "lines":lines ,
    "zones":zones ,
    "trendlines":trendlines ,
    "quality":signal_data .get ("quality",{}),

    "fvg_zones":fvg_zones ,
    "ob_zones":ob_zones ,
    "patterns":chart_patterns ,

    "pattern_confirmation":
    pattern_confirmation ,

    "reasons":
    signal_data ["reasons"],

    "mtf_matrix":mtf ,
    "pd_zones":pd ,

    "mss":mss ,
    "support_resistance":sr_levels ,
    "smc_commentary":commentary ,
    "smc_statistics":smc_statistics ,

    "atr":signal_data ["atr"],

    "bullish_mtf":
    signal_data ["bullish_mtf"],

    "bearish_mtf":
    signal_data ["bearish_mtf"],

    "confirmed_patterns":
    signal_data [
    "confirmed_patterns"
    ],

    "market_open":
    is_market_open (),

    "timestamp":
    now_utc_iso (),

    "offset":
    PRICE_OFFSET ,

    
    "signal_history":
    historical ["signals"],

    "fixed_entries":
    historical ["entries"],

    
    "active_entry":
    historical ["active_entry"],

    "last_signal_direction":
    historical ["last_direction"]
    }


    
    
    

import sqlite3 as _nm_sqlite3 
import json as _nm_json 
import re as _nm_re 
import urllib .parse as _nm_urlparse 
import xml .etree .ElementTree as _nm_ET 
from statistics import mean as _nm_mean 


NM_DB =os .path .join (
os .path .dirname (os .path .abspath (__file__ )),
"news_machine.db"
)

NM_EVENT_BEFORE_MINUTES =30 
NM_EVENT_AFTER_MINUTES =30 
NM_CELEBRATION_START =10 
NM_CELEBRATION_END =20 
NM_CALENDAR_REFRESH =300 
NM_CALENDAR_PREDICTION_REFRESH =30 


NM_HIST_MIN_SAMPLE =3 
NM_HIST_FULL_SAMPLE =8 
NM_MIN_MOVE =0.50 
NM_RESEARCH_REFRESH =180 
NM_MAX_HISTORICAL_MATCHES =50 

_nm_last_calendar =[]
_nm_calendar_time =0 
_nm_research_cache ={}
_nm_research_cache_time ={}
_nm_lock =threading .Lock ()


def _nm_db ():
    conn =_nm_sqlite3 .connect (
    NM_DB ,
    timeout =10 
    )

    conn .row_factory =_nm_sqlite3 .Row 

    return conn 


def _nm_init_db ():
    try :

        conn =_nm_db ()
        cur =conn .cursor ()

        cur .execute ("""
            CREATE TABLE IF NOT EXISTS news_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT UNIQUE,
                event_title TEXT,
                event_country TEXT,
                event_time TEXT,
                prediction TEXT,
                confidence REAL,
                evidence_score REAL,
                reason TEXT,
                research_summary TEXT,
                historical_matches INTEGER DEFAULT 0,
                price_before REAL,
                price_5m REAL,
                price_15m REAL,
                price_30m REAL,
                outcome TEXT,
                aligned INTEGER,
                created_at TEXT,
                completed_at TEXT
            )
        """)

        cur .execute ("""
            CREATE TABLE IF NOT EXISTS news_event_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT,
                event_title TEXT,
                event_time TEXT,
                actual REAL,
                forecast REAL,
                previous REAL,
                price_before REAL,
                price_5m REAL,
                price_15m REAL,
                price_30m REAL,
                direction TEXT,
                created_at TEXT
            )
        """)

        cur .execute ("""
            CREATE TABLE IF NOT EXISTS news_research (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT,
                source TEXT,
                title TEXT,
                url TEXT,
                published TEXT,
                summary TEXT,
                sentiment REAL,
                created_at TEXT
            )
        """)

        conn .commit ()
        conn .close ()

    except Exception as exc :
        print (
        f"[NEWS MACHINE DB] {exc }"
        )


_nm_init_db ()


def _nm_parse_time (value ):
    try :

        if not value :
            return None 

        text =str (value ).strip ()

        if text .endswith ("Z"):
            text =(
            text [:-1 ]
            +
            "+00:00"
            )

        dt =datetime .fromisoformat (
        text 
        )

        if dt .tzinfo is None :
            dt =dt .replace (
            tzinfo =timezone .utc 
            )

        return dt .astimezone (
        timezone .utc 
        )

    except :
        return None 


def _nm_event_key (event ):
    title =str (
    event .get (
    "title",
    ""
    )
    ).strip ().lower ()

    date =str (
    event .get (
    "date",
    ""
    )
    ).strip ()

    raw =f"{title }|{date }"

    return _nm_re .sub (
    r"[^a-z0-9|]+",
    "-",
    raw 
    )


def _nm_safe_number (value ):
    try :

        if value is None :
            return None 

        text =str (value ).strip ()

        if not text :
            return None 

        text =text .replace (
        ",",
        ""
        )

        text =text .replace (
        "%",
        ""
        )

        text =_nm_re .sub (
        r"[^0-9.\-+]",
        "",
        text 
        )

        if not text :
            return None 

        return float (text )

    except :
        return None 


def _nm_is_usd_event (event ):
    country =str (
    event .get (
    "country",
    ""
    )
    ).upper ()

    return country in {
    "USD",
    "US",
    "UNITED STATES",
    "USA"
    }


_CAL_FEED_URL ="https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_CAL_CACHE_FILE =os .path .join (
os .path .dirname (os .path .abspath (__file__ )),
"calendar_cache.json"
)
_CAL_FRESH_SECONDS =600 
_cal_json_cache ={"data":None ,"time":0 }
_cal_fail_until =0 
_cal_fetch_lock =threading .Lock ()


def _cal_load_disk ():
    """Load the last good calendar saved on disk (survives restarts)."""
    try :
        with open (_CAL_CACHE_FILE ,"r",encoding ="utf-8")as fh :
            saved =_nm_json .load (fh )
        if isinstance (saved .get ("data"),list ):
            return saved ["data"],float (saved .get ("time",0 ))
    except Exception :
        pass 
    return None ,0 


def _cal_save_disk (data ):
    try :
        with open (_CAL_CACHE_FILE ,"w",encoding ="utf-8")as fh :
            _nm_json .dump ({"data":data ,"time":time .time ()},fh )
    except Exception :
        pass 


def _fetch_calendar_json (force =False ):
    """Robust calendar feed fetch shared by the news popup and the
    News Analysis engine.

    - uses a FRESH connection (not the shared SESSION) to avoid SSL
      "bad record mac" errors from reused/shared connections
    - a good result is reused for 10 minutes and saved to disk
    - HTTP 429 (rate limit) is NOT retried; backs off 5 minutes
    - other failures retry once, then back off 60s
    - when the feed fails, the last good list (memory or disk) is
      returned so the site keeps showing news
    Returns the parsed list, or None only if no good list ever existed.
    """
    global _cal_fail_until 

    now =time .time ()

    if _cal_json_cache ["data"]is None :
        disk_data ,disk_time =_cal_load_disk ()
        if disk_data is not None :
            _cal_json_cache ["data"]=disk_data 
            _cal_json_cache ["time"]=disk_time 

    if (
    not force 
    and _cal_json_cache ["data"]is not None 
    and now -_cal_json_cache ["time"]<_CAL_FRESH_SECONDS 
    ):
        return _cal_json_cache ["data"]

    if now <_cal_fail_until :
        return _cal_json_cache ["data"]

    with _cal_fetch_lock :

        now =time .time ()

        if (
        not force 
        and _cal_json_cache ["data"]is not None 
        and now -_cal_json_cache ["time"]<_CAL_FRESH_SECONDS 
        ):
            return _cal_json_cache ["data"]

        if now <_cal_fail_until :
            return _cal_json_cache ["data"]

        backoff =60 

        for attempt in range (2 ):

            try :

                response =requests .get (
                _CAL_FEED_URL ,
                headers =HEADERS ,
                timeout =8 
                )

                if response .status_code ==200 :

                    data =response .json ()

                    if isinstance (data ,list ):

                        _cal_json_cache ["data"]=data 
                        _cal_json_cache ["time"]=time .time ()
                        _cal_save_disk (data )

                        return data 

                print (
                f"[NEWS FEED] HTTP {response .status_code } (attempt {attempt +1 })"
                )

                if response .status_code ==429 :

                    backoff =300 

                    try :
                        backoff =min (
                        max (
                        int (response .headers .get ("Retry-After",300 )),
                        60 
                        ),
                        900 
                        )
                    except Exception :
                        backoff =300 

                    break 

            except Exception as exc :

                print (
                f"[NEWS FEED] {exc } (attempt {attempt +1 })"
                )

            if attempt ==0 :
                time .sleep (1.0 )

        _cal_fail_until =time .time ()+backoff 

        return _cal_json_cache ["data"]


def _nm_get_calendar ():
    global _nm_last_calendar 
    global _nm_calendar_time 

    now =time .time ()

    with _nm_lock :
        refresh_interval =NM_CALENDAR_REFRESH 

        
        
        
        
        
        now_dt =datetime .now (timezone .utc )
        for cached_event in _nm_last_calendar :
            event_time =_nm_parse_time (cached_event .get ("date"))
            if event_time :
                minutes_until =(event_time -now_dt ).total_seconds ()/60 
                if 0 <minutes_until <=NM_EVENT_BEFORE_MINUTES :
                    refresh_interval =NM_CALENDAR_PREDICTION_REFRESH 
                    break 

        if (
        now -
        _nm_calendar_time 
        <
        refresh_interval 
        ):
            return list (_nm_last_calendar )

    try :

        data =_fetch_calendar_json (
        force =(refresh_interval ==NM_CALENDAR_PREDICTION_REFRESH )
        )

        if data is None :

        
        
            with _nm_lock :
                _nm_calendar_time =(
                now -NM_CALENDAR_REFRESH +60 
                )

            return list (
            _nm_last_calendar 
            )

        events =[]

        for raw in data :

            impact =raw .get (
            "impact",
            ""
            )

            country =raw .get (
            "country",
            ""
            )

            if str (
            impact 
            ).lower ()!="high":
                continue 

            if not _nm_is_usd_event ({
            "country":country 
            }):
                continue 

            events .append ({
            "id":raw .get ("id"),
            "title":raw .get (
            "title",
            ""
            ),
            "country":(
            country 
            or 
            "USD"
            ),
            "date":raw .get (
            "date",
            ""
            ),
            "impact":"High",
            "previous":raw .get (
            "previous"
            ),
            "forecast":raw .get (
            "forecast"
            ),
            "actual":raw .get (
            "actual"
            )
            })

        with _nm_lock :

            _nm_last_calendar =events 
            _nm_calendar_time =now 

        return events 

    except Exception as exc :

        print (
        f"[NEWS MACHINE CALENDAR] {exc }"
        )

        return list (
        _nm_last_calendar 
        )


def _nm_google_news (query ):
    try :

        encoded =_nm_urlparse .quote_plus (
        query 
        )

        url =(
        "https://news.google.com/rss/search?"
        f"q={encoded }&hl=en-US&gl=US&ceid=US:en"
        )

        response =SESSION .get (
        url ,
        timeout =7 
        )

        if response .status_code !=200 :
            return []

        root =_nm_ET .fromstring (
        response .text 
        )

        results =[]

        for item in root .findall (
        ".//item"
        )[:10 ]:

            title =(
            item .findtext ("title")
            or 
            ""
            )

            link =(
            item .findtext ("link")
            or 
            ""
            )

            published =(
            item .findtext ("pubDate")
            or 
            ""
            )

            description =(
            item .findtext ("description")
            or 
            ""
            )

            clean_description =_nm_re .sub (
            r"<[^>]+>",
            " ",
            description 
            )

            results .append ({
            "title":title .strip (),
            "url":link .strip (),
            "published":published .strip (),
            "summary":clean_description .strip ()[:800 ]
            })

        return results 

    except Exception as exc :

        print (
        f"[NEWS MACHINE RESEARCH] {exc }"
        )

        return []


def _nm_x_search (query ):
    token =os .environ .get (
    "X_BEARER_TOKEN",
    ""
    ).strip ()

    if not token :
        return []

    try :

        endpoint =(
        "https://api.x.com/2/tweets/search/recent"
        )

        params ={
        "query":(
        f"({query }) "
        "lang:en -is:retweet"
        ),
        "max_results":"25",
        "tweet.fields":(
        "created_at,"
        "public_metrics,text"
        )
        }

        headers ={
        "Authorization":
        f"Bearer {token }"
        }

        response =SESSION .get (
        endpoint ,
        params =params ,
        headers =headers ,
        timeout =7 
        )

        if response .status_code !=200 :

            print (
            f"[NEWS MACHINE X] API returned "
            f"{response .status_code }"
            )

            return []

        payload =response .json ()

        results =[]

        for tweet in payload .get (
        "data",
        []
        ):

            results .append ({
            "text":tweet .get (
            "text",
            ""
            ),
            "created_at":tweet .get (
            "created_at"
            ),
            "metrics":tweet .get (
            "public_metrics",
            {}
            )
            })

        return results 

    except Exception as exc :

        print (
        f"[NEWS MACHINE X] {exc }"
        )

        return []


_NM_BULLISH_WORDS ={
"bullish",
"surge",
"surges",
"rally",
"rallies",
"higher",
"rise",
"rising",
"strong",
"hawkish",
"inflation",
"safe haven",
"geopolitical",
"uncertainty",
"dovish",
"rate cut",
"cuts rates"
}


_NM_BEARISH_WORDS ={
"bearish",
"drop",
"drops",
"fall",
"falling",
"lower",
"weak",
"selloff",
"sell-off",
"hawkish dollar",
"strong dollar",
"higher yields",
"yield surge",
"rate hike"
}


def _nm_sentiment (text ):
    text =str (
    text or ""
    ).lower ()

    bull =sum (
    1 
    for word in _NM_BULLISH_WORDS 
    if word in text 
    )

    bear =sum (
    1 
    for word in _NM_BEARISH_WORDS 
    if word in text 
    )

    total =bull +bear 

    if total ==0 :
        return 0.0 

    return round (
    (bull -bear )/total ,
    3 
    )


def _nm_research_event (event ):
    key =_nm_event_key (
    event 
    )

    now =time .time ()

    if (
    key in _nm_research_cache 
    and 
    now -
    _nm_research_cache_time .get (
    key ,
    0 
    )
    <
    NM_RESEARCH_REFRESH 
    ):
        return _nm_research_cache [key ]

    title =str (
    event .get (
    "title",
    ""
    )
    ).strip ()

    queries =[
    f"{title } gold XAUUSD",
    f"{title } Federal Reserve dollar gold",
    f"{title } USD Treasury yields gold"
    ]

    articles =[]

    for query in queries :
        articles .extend (
        _nm_google_news (
        query 
        )
        )

    unique ={}

    for article in articles :

        url =article .get (
        "url",
        ""
        )

        if url :
            unique [url ]=article 

    articles =list (
    unique .values ()
    )[:20 ]

    article_scores =[
    _nm_sentiment (
    article .get (
    "title",
    ""
    )
    +
    " "
    +
    article .get (
    "summary",
    ""
    )
    )
    for article in articles 
    ]

    news_sentiment =(
    round (
    _nm_mean (
    article_scores 
    ),
    3 
    )
    if article_scores 
    else None 
    )

    x_posts =_nm_x_search (
    f'"{title }" gold OR XAUUSD'
    )

    x_scores =[
    _nm_sentiment (
    post .get (
    "text",
    ""
    )
    )
    for post in x_posts 
    ]

    x_sentiment =(
    round (
    _nm_mean (
    x_scores 
    ),
    3 
    )
    if x_scores 
    else None 
    )

    result ={
    "articles":articles ,
    "x_posts_count":len (x_posts ),
    "news_sentiment":news_sentiment ,
    "x_sentiment":x_sentiment ,
    "research_available":bool (
    articles or x_posts 
    )
    }

    _nm_research_cache [key ]=result 
    _nm_research_cache_time [key ]=now 

    try :

        conn =_nm_db ()

        for article in articles [:20 ]:

            conn .execute ("""
                INSERT INTO news_research
                (query, source, title, url, published, summary,
                 sentiment, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,(
            title ,
            "Google News",
            article .get (
            "title",
            ""
            ),
            article .get (
            "url",
            ""
            ),
            article .get (
            "published",
            ""
            ),
            article .get (
            "summary",
            ""
            ),
            _nm_sentiment (
            article .get (
            "title",
            ""
            )
            +
            " "
            +
            article .get (
            "summary",
            ""
            )
            ),
            now_utc_iso ()
            ))

        conn .commit ()
        conn .close ()

    except Exception as exc :

        print (
        f"[NEWS MACHINE RESEARCH DB] {exc }"
        )

    return result 


def _nm_event_family (event ):
    """Normalize a calendar title into a stable economic event family.

    This is used only by the News Machine so historical reactions are
    compared with the same kind of release instead of relying only on
    loose title-word overlap.
    """
    title =str (event .get ("title","")).lower ()

    families =[
    ("nfp",["nonfarm","non-farm","non farm","payroll"]),
    ("unemployment",["unemployment rate"]),
    ("jobless_claims",["initial claims","continuing claims","jobless claims"]),
    ("average_hourly_earnings",["average hourly earnings","hourly earnings"]),
    ("cpi",["consumer price index","cpi"]),
    ("core_cpi",["core cpi","core consumer price"]),
    ("ppi",["producer price index","ppi"]),
    ("core_ppi",["core ppi"]),
    ("pce",["personal consumption expenditures","pce"]),
    ("core_pce",["core pce"]),
    ("retail_sales",["retail sales"]),
    ("core_retail_sales",["core retail sales"]),
    ("gdp",["gross domestic product","gdp"]),
    ("ism_manufacturing",["ism manufacturing","manufacturing pmi"]),
    ("ism_services",["ism services","services pmi"]),
    ("consumer_confidence",["consumer confidence"]),
    ("consumer_sentiment",["consumer sentiment"]),
    ("durable_goods",["durable goods"]),
    ("industrial_production",["industrial production"]),
    ("fed_rate",["federal funds rate","fed interest rate","interest rate decision","fomc"]),
    ("fed_minutes",["fomc minutes","fed minutes"]),
    ("trade_balance",["trade balance"]),
    ("housing",["housing starts","building permits","existing home sales","new home sales"]),
    ]

    for family ,needles in families :
        if any (needle in title for needle in needles ):
            return family 

            
            
    words =[
    w for w in _nm_re .findall (r"[a-z0-9]+",title )
    if len (w )>2 and w not in {"actual","forecast","previous","month","year"}
    ]

    return "generic:"+"_".join (words [:5 ])if words else "generic"


def _nm_event_numeric_context (event ):
    forecast =_nm_safe_number (event .get ("forecast"))
    previous =_nm_safe_number (event .get ("previous"))

    if forecast is None or previous is None :
        return None 

    scale =max (abs (previous ),abs (forecast ),1.0 )
    change =forecast -previous 

    return {
    "forecast":forecast ,
    "previous":previous ,
    "change":change ,
    "normalized_change":change /scale 
    }


def _nm_direction_from_macro (family ,change ):
    """Translate expected macro direction into its usual gold bias.

    This is a pre-release expectation only. Actual is deliberately never
    used here because the prediction must be made before the release.
    """
    if change is None or abs (change )<0.000001 :
        return None 

    bearish_gold ={
    "nfp","average_hourly_earnings","cpi","core_cpi","ppi","core_ppi",
    "pce","core_pce","retail_sales","core_retail_sales","gdp",
    "ism_manufacturing","ism_services","consumer_confidence",
    "consumer_sentiment","durable_goods","industrial_production",
    "fed_rate","trade_balance","housing"
    }

    bullish_gold ={
    "unemployment","jobless_claims"
    }

    if family in bullish_gold :
        return "BUY"if change >0 else "SELL"

    if family in bearish_gold :
        return "SELL"if change >0 else "BUY"

    return None 


def _nm_trend_from_closes (closes ,fast =8 ,slow =21 ):
    if len (closes )<slow :
        return "NEUTRAL"

    fast_avg =_nm_mean (closes [-fast :])
    slow_avg =_nm_mean (closes [-slow :])

    if fast_avg >slow_avg :
        return "BULLISH"
    if fast_avg <slow_avg :
        return "BEARISH"
    return "NEUTRAL"


def _nm_market_context ():
    try :
        candles_5m =get_gold_with_live ("5m")
        candles_15m =get_gold_with_live ("15m")
        candles_1h =get_gold_with_live ("1h")

        if len (candles_5m )<30 :
            return {"available":False }

        closes_5m =[c ["close"]for c in candles_5m ]
        closes_15m =[c ["close"]for c in candles_15m ]
        closes_1h =[c ["close"]for c in candles_1h ]

        price =closes_5m [-1 ]
        short =_nm_mean (closes_5m [-10 :])
        medium =_nm_mean (closes_5m [-20 :])
        rsi =calculate_rsi (closes_5m )
        atr =calculate_atr (candles_5m )
        structure =detect_structure (candles_5m )

        try :
            smc =smc_analysis ("5m")
        except Exception :
            smc ={}

        trend_5m =_nm_trend_from_closes (closes_5m ,8 ,21 )
        trend_15m =_nm_trend_from_closes (closes_15m ,6 ,18 )
        trend_1h =_nm_trend_from_closes (closes_1h ,5 ,15 )

        
        
        
        current_atr =calculate_atr (candles_5m [-30 :],14 )
        previous_atr =calculate_atr (candles_5m [-45 :-15 ],14 )
        volatility_ratio =(
        current_atr /previous_atr 
        if previous_atr >0 
        else 1.0 
        )

        if volatility_ratio >=1.50 :
            volatility ="EXTREME"
        elif volatility_ratio >=1.20 :
            volatility ="HIGH"
        elif volatility_ratio <=0.80 :
            volatility ="LOW"
        else :
            volatility ="NORMAL"

        bullish_tf =sum (
        1 for trend in [trend_1h ,trend_15m ,trend_5m ]
        if trend =="BULLISH"
        )
        bearish_tf =sum (
        1 for trend in [trend_1h ,trend_15m ,trend_5m ]
        if trend =="BEARISH"
        )

        if bullish_tf >=2 and bullish_tf >bearish_tf :
            mtf_bias ="BUY"
        elif bearish_tf >=2 and bearish_tf >bullish_tf :
            mtf_bias ="SELL"
        else :
            mtf_bias =None 

        return {
        "available":True ,
        "price":round (price ,2 ),
        "short_average":round (short ,2 ),
        "medium_average":round (medium ,2 ),
        "rsi":rsi ,
        "atr":atr ,
        "trend":trend_5m ,
        "trend_5m":trend_5m ,
        "trend_15m":trend_15m ,
        "trend_1h":trend_1h ,
        "mtf_bias":mtf_bias ,
        "bullish_timeframes":bullish_tf ,
        "bearish_timeframes":bearish_tf ,
        "volatility":volatility ,
        "volatility_ratio":round (volatility_ratio ,3 ),
        "structure":structure ,
        "smc_signal":smc .get ("signal"),
        "smc_score":smc .get ("score"),
        "smc_confidence":smc .get ("confidence")
        }

    except Exception as exc :
        print (f"[NEWS MACHINE MARKET] {exc }")
        return {"available":False }


def _nm_find_historical_matches (event ):
    family =_nm_event_family (event )
    numeric =_nm_event_numeric_context (event )
    title =str (event .get ("title","")).lower ()

    try :
        conn =_nm_db ()
        rows =conn .execute ("""
            SELECT *
            FROM news_event_history
            WHERE direction IN ('BUY', 'SELL')
            ORDER BY event_time DESC
            LIMIT 500
        """).fetchall ()
        conn .close ()
    except Exception :
        return []

    words ={
    w for w in _nm_re .findall (r"[a-z0-9]+",title )
    if len (w )>2 
    }

    scored =[]

    for row in rows :
        old =dict (row )
        old_family =_nm_event_family ({"title":old .get ("event_title","")})

        if old_family ==family :
            similarity =1.0 
        else :
            old_words ={
            w for w in _nm_re .findall (r"[a-z0-9]+",str (old .get ("event_title","")).lower ())
            if len (w )>2 
            }
            overlap =len (words &old_words )
            similarity =overlap /max (len (words ),1 )

        if similarity <0.45 :
            continue 

            
        surprise_similarity =0.0 
        if numeric :
            old_forecast =_nm_safe_number (old .get ("forecast"))
            old_previous =_nm_safe_number (old .get ("previous"))
            if old_forecast is not None and old_previous is not None :
                old_scale =max (abs (old_forecast ),abs (old_previous ),1.0 )
                old_change =(old_forecast -old_previous )/old_scale 
                difference =abs (
                numeric ["normalized_change"]-old_change 
                )
                surprise_similarity =max (0.0 ,1.0 -min (difference *5.0 ,1.0 ))

        score =(similarity *0.70 )+(surprise_similarity *0.30 )
        old ["_match_score"]=round (score ,4 )
        old ["_surprise_similarity"]=round (surprise_similarity ,4 )
        scored .append (old )

    scored .sort (key =lambda item :item .get ("_match_score",0 ),reverse =True )
    return scored [:NM_MAX_HISTORICAL_MATCHES ]


def _nm_historical_bias (matches ):
    if not matches :
        return {"direction":None ,"strength":0 ,"sample":0 ,"buy_weight":0 ,"sell_weight":0 }

    buy_weight =0.0 
    sell_weight =0.0 

    for match in matches :
        weight =float (match .get ("_match_score",1.0 )or 1.0 )
        direction =str (match .get ("direction")or "").upper ()
        if direction =="BUY":
            buy_weight +=weight 
        elif direction =="SELL":
            sell_weight +=weight 

    total =buy_weight +sell_weight 
    if total <=0 :
        return {"direction":None ,"strength":0 ,"sample":0 ,"buy_weight":0 ,"sell_weight":0 }

    if buy_weight >sell_weight :
        direction ="BUY"
        strength =buy_weight /total 
    elif sell_weight >buy_weight :
        direction ="SELL"
        strength =sell_weight /total 
    else :
        direction =None 
        strength =0 

    return {
    "direction":direction ,
    "strength":round (strength ,3 ),
    "sample":len (matches ),
    "buy_weight":round (buy_weight ,3 ),
    "sell_weight":round (sell_weight ,3 )
    }


def _nm_macro_context (event ):
    family =_nm_event_family (event )
    numeric =_nm_event_numeric_context (event )

    if not numeric :
        return {
        "direction":None ,
        "strength":0 ,
        "family":family ,
        "forecast":None ,
        "previous":None ,
        "expected_change":None ,
        "normalized_change":None 
        }

    direction =_nm_direction_from_macro (
    family ,
    numeric ["change"]
    )

    
    
    
    magnitude =min (
    abs (numeric ["normalized_change"])*10.0 ,
    1.0 
    )
    strength =round (
    0.30 +(0.70 *magnitude ),
    3 
    )if direction else 0 

    return {
    "direction":direction ,
    "strength":strength ,
    "family":family ,
    "forecast":numeric ["forecast"],
    "previous":numeric ["previous"],
    "expected_change":numeric ["change"],
    "normalized_change":numeric ["normalized_change"]
    }


def _nm_historical_prediction_performance (event ):
    family =_nm_event_family (event )

    try :
        conn =_nm_db ()
        rows =conn .execute ("""
            SELECT event_title, prediction, outcome
            FROM news_predictions
            WHERE outcome IN ('WIN', 'LOSS')
            ORDER BY completed_at DESC
            LIMIT 500
        """).fetchall ()
        conn .close ()
    except Exception :
        return {"sample":0 ,"wins":0 ,"losses":0 ,"win_rate":None }

    wins =0 
    losses =0 

    for row in rows :
        if _nm_event_family ({"title":row ["event_title"]})!=family :
            continue 
        if str (row ["outcome"]).upper ()=="WIN":
            wins +=1 
        elif str (row ["outcome"]).upper ()=="LOSS":
            losses +=1 

    sample =wins +losses 
    return {
    "sample":sample ,
    "wins":wins ,
    "losses":losses ,
    "win_rate":round ((wins /sample )*100 ,2 )if sample else None 
    }


def _nm_make_prediction (event ):
    research =_nm_research_event (event )
    market =_nm_market_context ()
    historical_matches =_nm_find_historical_matches (event )
    historical =_nm_historical_bias (historical_matches )
    macro =_nm_macro_context (event )
    performance =_nm_historical_prediction_performance (event )

    buy_score =0.0 
    sell_score =0.0 
    reasons =[]

    
    
    hist_used =historical ["sample"]>=NM_HIST_MIN_SAMPLE 
    hist_scale =min (
    historical ["sample"]/NM_HIST_FULL_SAMPLE ,
    1.0 
    )

    if hist_used and historical ["direction"]=="BUY":
        weight =4.5 *historical ["strength"]*hist_scale 
        buy_score +=weight 
        reasons .append (
        f"Historical same-event evidence: {historical ['sample']} relevant matches favour BUY"
        )

    elif hist_used and historical ["direction"]=="SELL":
        weight =4.5 *historical ["strength"]*hist_scale 
        sell_score +=weight 
        reasons .append (
        f"Historical same-event evidence: {historical ['sample']} relevant matches favour SELL"
        )

        
    if market .get ("mtf_bias")=="BUY":
        buy_score +=2.0 
        reasons .append ("1H/15M/5M market context is predominantly bullish")
    elif market .get ("mtf_bias")=="SELL":
        sell_score +=2.0 
        reasons .append ("1H/15M/5M market context is predominantly bearish")
    else :
        if market .get ("trend_5m")=="BULLISH":
            buy_score +=0.75 
        elif market .get ("trend_5m")=="BEARISH":
            sell_score +=0.75 

    smc_signal =str (market .get ("smc_signal")or "").upper ()
    if smc_signal in {"BUY","STRONG BUY"}:
        buy_score +=1.5 
        reasons .append ("Existing SMC engine agrees with BUY")
    elif smc_signal in {"SELL","STRONG SELL"}:
        sell_score +=1.5 
        reasons .append ("Existing SMC engine agrees with SELL")

    structure_state =str (
    market .get ("structure",{}).get ("state","")
    ).upper ()
    if structure_state =="BULLISH":
        buy_score +=1.0 
        reasons .append ("Current market structure is bullish")
    elif structure_state =="BEARISH":
        sell_score +=1.0 
        reasons .append ("Current market structure is bearish")

    rsi =safe_float (market .get ("rsi"),50 )
    if 52 <=rsi <=68 :
        buy_score +=0.60 
        reasons .append ("RSI supports bullish momentum")
    elif 32 <=rsi <=48 :
        sell_score +=0.60 
        reasons .append ("RSI supports bearish momentum")

    if macro ["direction"]=="BUY":
        buy_score +=macro ["strength"]
        reasons .append (
        f"Expected {macro ['family']} release bias favours BUY from forecast/previous"
        )
    elif macro ["direction"]=="SELL":
        sell_score +=macro ["strength"]
        reasons .append (
        f"Expected {macro ['family']} release bias favours SELL from forecast/previous"
        )

    news_sentiment =research .get ("news_sentiment")
    if news_sentiment is not None :
        if news_sentiment >0.20 :
            buy_score +=0.60 
            reasons .append ("Recent online news research has a bullish Gold bias")
        elif news_sentiment <-0.20 :
            sell_score +=0.60 
            reasons .append ("Recent online news research has a bearish Gold bias")

    x_sentiment =research .get ("x_sentiment")
    if x_sentiment is not None :
        if x_sentiment >0.25 :
            buy_score +=0.35 
            reasons .append ("Recent X research provides bullish context")
        elif x_sentiment <-0.25 :
            sell_score +=0.35 
            reasons .append ("Recent X research provides bearish context")

            
            
    volatility =market .get ("volatility")
    if volatility =="EXTREME":
        reasons .append ("Pre-news gold volatility is extreme")
    elif volatility =="HIGH":
        reasons .append ("Pre-news gold volatility is high")

    if buy_score >=sell_score :
        prediction ="BUY"
        winning_score =buy_score 
        losing_score =sell_score 
    else :
        prediction ="SELL"
        winning_score =sell_score 
        losing_score =buy_score 

    total =buy_score +sell_score 
    separation =(
    abs (winning_score -losing_score )/total 
    if total >0 else 0 
    )

    confidence =50 +(separation *38 )

    evidence_count =sum ([
    1 if hist_used else 0 ,
    1 if market .get ("available")else 0 ,
    1 if market .get ("mtf_bias")else 0 ,
    1 if smc_signal in {"BUY","SELL","STRONG BUY","STRONG SELL"}else 0 ,
    1 if structure_state in {"BULLISH","BEARISH"}else 0 ,
    1 if news_sentiment is not None else 0 ,
    1 if x_sentiment is not None else 0 ,
    1 if macro ["direction"]else 0 
    ])

    confidence +=min (evidence_count *1.25 ,8 )

    
    
    if performance ["sample"]>=5 and performance ["win_rate"]is not None :
        confidence +=clamp (
        (performance ["win_rate"]-50.0 )*0.10 ,
        -4.0 ,
        4.0 
        )
        reasons .append (
        f"Historical {macro ['family']} prediction sample: {performance ['sample']} completed results"
        )

        
        
    if market .get ("bullish_timeframes",0 )>0 and market .get ("bearish_timeframes",0 )>0 :
        confidence -=2.0 
        reasons .append ("Multi-timeframe market evidence is mixed")

    if volatility =="EXTREME":
        confidence -=2.0 

    confidence =round (clamp (confidence ,50 ,97 ),1 )

    return {
    "prediction":prediction ,
    "confidence":confidence ,
    "evidence_score":round (max (buy_score ,sell_score ),2 ),
    "buy_score":round (buy_score ,2 ),
    "sell_score":round (sell_score ,2 ),
    "reason":" | ".join (reasons [:10 ]),
    "historical_matches":historical ["sample"],
    "historical_bias":historical ["direction"],
    "historical_strength":historical ["strength"],
    "historical_performance":performance ,
    "event_family":macro ["family"],
    "expected_macro":macro ,
    "news_sentiment":news_sentiment ,
    "x_sentiment":x_sentiment ,
    "market":market ,
    "research_available":research ["research_available"]
    }

def _nm_get_prediction (event_key ):
    try :

        conn =_nm_db ()

        row =conn .execute ("""
            SELECT *
            FROM news_predictions
            WHERE event_key = ?
        """,(
        event_key ,
        )).fetchone ()

        conn .close ()

        return (
        dict (row )
        if row 
        else None 
        )

    except Exception :
        return None 


def _nm_save_prediction (
event ,
prediction 
):
    try :

        event_key =_nm_event_key (
        event 
        )

        if _nm_get_prediction (
        event_key 
        ):
            return 

        market =prediction .get (
        "market",
        {}
        )

        price =market .get (
        "price"
        )

        conn =_nm_db ()

        conn .execute ("""
            INSERT OR IGNORE INTO news_predictions
            (
                event_key,
                event_title,
                event_country,
                event_time,
                prediction,
                confidence,
                evidence_score,
                reason,
                research_summary,
                historical_matches,
                price_before,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,(
        event_key ,
        event .get (
        "title",
        ""
        ),
        event .get (
        "country",
        "USD"
        ),
        event .get (
        "date",
        ""
        ),
        prediction [
        "prediction"
        ],
        prediction [
        "confidence"
        ],
        prediction [
        "evidence_score"
        ],
        prediction [
        "reason"
        ],
        _nm_json .dumps ({
        "news_sentiment":prediction .get ("news_sentiment"),
        "x_sentiment":prediction .get ("x_sentiment"),
        "research_available":prediction .get ("research_available"),
        "event_family":prediction .get ("event_family"),
        "expected_macro":prediction .get ("expected_macro"),
        "historical_performance":prediction .get ("historical_performance"),
        "market":prediction .get ("market",{})
        }),
        prediction .get (
        "historical_matches",
        0 
        ),
        price ,
        now_utc_iso ()
        ))

        conn .commit ()
        conn .close ()

    except Exception as exc :

        print (
        f"[NEWS MACHINE SAVE] {exc }"
        )


def _nm_capture_outcomes ():
    try :

        conn =_nm_db ()

        rows =conn .execute ("""
            SELECT *
            FROM news_predictions
            WHERE outcome IS NULL
            ORDER BY id DESC
            LIMIT 50
        """).fetchall ()

        for row in rows :

            event_time =_nm_parse_time (
            row ["event_time"]
            )

            if not event_time :
                continue 

            now =datetime .now (
            timezone .utc 
            )

            minutes_after =(
            now -event_time 
            ).total_seconds ()/60 

            if (
            minutes_after 
            <
            NM_EVENT_AFTER_MINUTES 
            ):
                continue 

            candles =get_gold (
            "5m"
            )

            if not candles :
                continue 

            event_ts =(
            event_time .timestamp ()
            )

            before =None 
            after_5 =None 
            after_15 =None 
            after_30 =None 

            
            
            
            
            for candle in candles :

                t =candle ["time"]
                candle_close_time =t +5 *60 

                if candle_close_time <=event_ts :
                    before =candle ["close"]

                if (
                t >=event_ts 
                and candle_close_time 
                <=event_ts +5 *60 
                ):
                    after_5 =candle ["close"]

                if (
                t >=event_ts 
                and candle_close_time 
                <=event_ts +15 *60 
                ):
                    after_15 =candle ["close"]

                if (
                t >=event_ts 
                and candle_close_time 
                <=event_ts +30 *60 
                ):
                    after_30 =candle ["close"]

            if (
            before is None 
            or 
            after_30 is None 
            ):
                continue 

            prediction =str (
            row ["prediction"]
            ).upper ()

            if abs (after_30 -before )<NM_MIN_MOVE :
            
                actual_direction =None 

            elif after_30 >before :
                actual_direction ="BUY"

            else :
                actual_direction ="SELL"

            if actual_direction ==prediction :

                outcome ="WIN"
                aligned =1 

            elif actual_direction :

                outcome ="LOSS"
                aligned =0 

            else :

                outcome ="UNRESOLVED"
                aligned =0 

            conn .execute ("""
                UPDATE news_predictions
                SET
                    price_5m = ?,
                    price_15m = ?,
                    price_30m = ?,
                    outcome = ?,
                    aligned = ?,
                    completed_at = ?
                WHERE id = ?
            """,(
            after_5 ,
            after_15 ,
            after_30 ,
            outcome ,
            aligned ,
            now_utc_iso (),
            row ["id"]
            ))

            
            
            
            event_details ={}
            try :
                calendar_events =_nm_get_calendar ()
                for calendar_event in calendar_events :
                    if _nm_event_key (calendar_event )==row ["event_key"]:
                        event_details =calendar_event 
                        break 
            except Exception :
                event_details ={}

            conn .execute ("""
                INSERT INTO news_event_history
                (
                    event_key,
                    event_title,
                    event_time,
                    actual,
                    forecast,
                    previous,
                    price_before,
                    price_5m,
                    price_15m,
                    price_30m,
                    direction,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,(
            row ["event_key"],
            row ["event_title"],
            row ["event_time"],
            event_details .get ("actual"),
            event_details .get ("forecast"),
            event_details .get ("previous"),
            before ,
            after_5 ,
            after_15 ,
            after_30 ,
            actual_direction ,
            now_utc_iso ()
            ))

        conn .commit ()
        conn .close ()

    except Exception as exc :

        print (
        f"[NEWS MACHINE OUTCOME] {exc }"
        )


def _nm_get_current ():
    events =_nm_get_calendar ()

    now =datetime .now (
    timezone .utc 
    )

    high_events_today =[]

    for event in events :

        event_time =_nm_parse_time (
        event .get (
        "date"
        )
        )

        if not event_time :
            continue 

        if event_time .date ()!=now .date ():
            continue 

        high_events_today .append (
        (
        event ,
        event_time 
        )
        )

    high_events_today .sort (
    key =lambda x :x [1 ]
    )

    for event ,event_time in high_events_today :

        minutes_until =(
        event_time -now 
        ).total_seconds ()/60 

        if (
        0 <
        minutes_until 
        <=
        NM_EVENT_BEFORE_MINUTES 
        ):

        
        
        
            stored =_nm_get_prediction (
            _nm_event_key (event )
            )

            if stored :

                prediction =stored 

            else :

                prediction =_nm_make_prediction (
                event 
                )

                _nm_save_prediction (
                event ,
                prediction 
                )

                stored =_nm_get_prediction (
                _nm_event_key (event )
                )

                if stored :
                    prediction =stored 

            return {
            "status":"PREDICTION",
            "event":event ,
            "prediction":prediction [
            "prediction"
            ],
            "confidence":prediction [
            "confidence"
            ],
            "evidence_score":prediction [
            "evidence_score"
            ],
            "reason":prediction [
            "reason"
            ],
            "historical_matches":prediction [
            "historical_matches"
            ],
            "aligned":False ,
            "celebration":False ,
            "server_time":now_utc_iso ()
            }

        minutes_after =(
        now -event_time 
        ).total_seconds ()/60 

        if (
        0 <=minutes_after 
        <=NM_EVENT_AFTER_MINUTES 
        ):

            stored =_nm_get_prediction (
            _nm_event_key (event )
            )

            aligned =False 
            celebration =False 

            if stored :

                aligned =(
                stored .get (
                "aligned"
                )==1 
                )

                celebration =(
                aligned 
                and 
                NM_CELEBRATION_START 
                <=minutes_after 
                <=NM_CELEBRATION_END 
                )

            later_events =[
            e 
            for e ,t 
            in high_events_today 
            if t >event_time 
            ]

            return {
            "status":"POST-NEWS MONITORING",
            "event":event ,
            
            
            "prediction":(
            stored .get (
            "prediction"
            )
            if stored 
            else None 
            ),
            "confidence":(
            stored .get (
            "confidence"
            )
            if stored 
            else None 
            ),
            "evidence_score":(
            stored .get (
            "evidence_score"
            )
            if stored 
            else None 
            ),
            "reason":(
            stored .get (
            "reason"
            )
            if stored 
            else None 
            ),
            "historical_matches":(
            stored .get (
            "historical_matches",
            0 
            )
            if stored 
            else 0 
            ),
            "aligned":aligned ,
            "celebration":celebration ,
            "server_time":now_utc_iso ()
            }

    future_events =[
    (
    event ,
    event_time 
    )
    for event ,event_time 
    in high_events_today 
    if event_time >now 
    ]

    if future_events :

        event ,event_time =future_events [0 ]

        return {
        "status":"ANALYSIS ONGOING",
        "event":event ,
        "prediction":None ,
        "confidence":None ,
        "evidence_score":None ,
        "reason":None ,
        "historical_matches":0 ,
        "aligned":False ,
        "celebration":False ,
        "server_time":now_utc_iso ()
        }

    return {
    "status":"NO NEWS",
    "event":None ,
    "prediction":None ,
    "confidence":None ,
    "evidence_score":None ,
    "reason":None ,
    "historical_matches":0 ,
    "aligned":False ,
    "celebration":False ,
    "server_time":now_utc_iso ()
    }


def _nm_statistics ():
    try :

        conn =_nm_db ()

        
        
        
        total_events =conn .execute ("""
            SELECT COUNT(DISTINCT event_key)
            FROM news_predictions
        """).fetchone ()[0 ]

        predictions =conn .execute ("""
            SELECT COUNT(*)
            FROM news_predictions
        """).fetchone ()[0 ]

        wins =conn .execute ("""
            SELECT COUNT(*)
            FROM news_predictions
            WHERE outcome = 'WIN'
        """).fetchone ()[0 ]

        losses =conn .execute ("""
            SELECT COUNT(*)
            FROM news_predictions
            WHERE outcome = 'LOSS'
        """).fetchone ()[0 ]

        avg_confidence =conn .execute ("""
            SELECT AVG(confidence)
            FROM news_predictions
            WHERE outcome IN ('WIN', 'LOSS')
        """).fetchone ()[0 ]

        conn .close ()

        completed =wins +losses 

        win_rate =(
        round (
        (wins /completed )*100 ,
        2 
        )
        if completed 
        else None 
        )

        avg_confidence =(
        round (
        float (avg_confidence ),
        2 
        )
        if avg_confidence is not None 
        else None 
        )

        return {
        "total_events":total_events ,
        "predictions":predictions ,
        "signals":predictions ,
        "wins":wins ,
        "losses":losses ,
        "win_rate":win_rate ,
        "average_confidence":avg_confidence 
        }

    except Exception as exc :

        print (
        f"[NEWS MACHINE STATS] {exc }"
        )

        return {
        "total_events":0 ,
        "predictions":0 ,
        "signals":0 ,
        "wins":0 ,
        "losses":0 ,
        "win_rate":None ,
        "average_confidence":None 
        }


def _nm_background_worker ():

    while True :

        try :

            _nm_get_calendar ()
            _nm_capture_outcomes ()

            events =_nm_get_calendar ()

            now =datetime .now (
            timezone .utc 
            )

            for event in events :

                event_time =_nm_parse_time (
                event .get (
                "date"
                )
                )

                if not event_time :
                    continue 

                minutes_until =(
                event_time -now 
                ).total_seconds ()/60 

                if (
                0 <
                minutes_until 
                <=
                NM_EVENT_BEFORE_MINUTES 
                ):

                    try :

                        _nm_research_event (
                        event 
                        )

                    except Exception :
                        pass 

            time .sleep (20 )

        except Exception as exc :

            print (
            f"[NEWS MACHINE WORKER] {exc }"
            )

            time .sleep (20 )


threading .Thread (
target =_nm_background_worker ,
daemon =True 
).start ()






@app .route (
"/api/news-analysis"
)
def api_news_analysis ():

    try :

        return jsonify (
        _nm_get_current ()
        )

    except Exception as exc :

        print (
        f"[NEWS MACHINE API] {exc }"
        )

        return jsonify ({
        "status":"ANALYSIS ONGOING",
        "event":None ,
        "prediction":None ,
        "confidence":None ,
        "evidence_score":None ,
        "reason":None ,
        "historical_matches":0 ,
        "aligned":False ,
        "celebration":False ,
        "server_time":now_utc_iso ()
        })


@app .route (
"/api/news-analysis/statistics"
)
def api_news_analysis_statistics ():

    return jsonify (
    _nm_statistics ()
    )


    
    
    

_news_cache ={
"data":[],
"time":0 
}


@app .route (
"/api/news"
)
def api_news ():

    now =time .time ()

    if (
    now -
    _news_cache ["time"]
    <
    300 
    ):
        return jsonify (
        _news_cache ["data"]
        )

    try :

        data =_fetch_calendar_json ()

        if data is not None :

            high =[]

            for ev in data :

                if (
                str (ev .get ("impact","")).strip ().lower ()
                ==
                "high"
                and 
                str (ev .get ("country","")).strip ().upper ()
                in 
                [
                "USD",
                "US"
                ]
                ):

                    high .append ({
                    "id":ev .get (
                    "id"
                    ),
                    "title":ev .get (
                    "title",
                    ""
                    ),
                    "date":ev .get (
                    "date",
                    ""
                    ),
                    "country":ev .get (
                    "country",
                    "USD"
                    ),
                    "impact":"High",
                    "previous":ev .get (
                    "previous"
                    ),
                    "forecast":ev .get (
                    "forecast"
                    ),
                    "actual":ev .get (
                    "actual"
                    )
                    })

            _news_cache ["data"]=high [:5 ]
            _news_cache ["time"]=now 

            return jsonify (
            high [:5 ]
            )

    except Exception as e :

        print (
        f"[NEWS] {e }"
        )

        
    _news_cache ["time"]=now -300 +60 

    if _news_cache ["data"]:

        return jsonify (
        _news_cache ["data"]
        )

        
        
    return jsonify ([]),503 


@app .route ("/robots.txt")
def robots_txt ():
    return (
    "User-agent: *\n"
    "Allow: /\n"
    "\n"
    "Sitemap: https://www.xauusd-smc-analysis.publicvm.com/sitemap.xml\n",
    200 ,
    {"Content-Type":"text/plain"}
    )


@app .route ("/sitemap.xml")
def sitemap_xml ():
    return (
    """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
        <loc>https://www.xauusd-smc-analysis.publicvm.com/</loc>
    </url>
</urlset>""",
    200 ,
    {"Content-Type":"application/xml"}
    )


@app .route ("/")
def index ():
    return render_template (
    "index.html"
    )


@app .route (
"/api/candles"
)
def api_candles ():

    return jsonify (
    get_gold_with_live (
    request .args .get (
    "tf",
    "5m"
    )
    )
    )


@app .route (
"/api/analysis"
)
def api_analysis ():

    return jsonify (
    smc_analysis (
    request .args .get (
    "tf",
    "5m"
    )
    )
    )


@app .route (
"/api/price"
)
def api_price ():

    tf =request .args .get (
    "tf",
    "5m"
    )

    
    
    
    
    live =get_oanda_live ()

    live_is_fresh =(
    live 
    and live >1000 
    and (time .time ()-_last_live_fetch )<15 
    )

    if live_is_fresh :

        return jsonify ({
        "price":round (live ,2 ),
        "market_open":is_market_open (),
        "timestamp":now_utc_iso (),
        "offset":PRICE_OFFSET 
        })

    candles =get_gold_with_live (
    tf 
    )

    if not candles :

        return jsonify ({
        "price":0 ,
        "market_open":is_market_open (),
        "offset":PRICE_OFFSET 
        })

    return jsonify ({
    "price":candles [-1 ]["close"],
    "market_open":is_market_open (),
    "timestamp":now_utc_iso (),
    "offset":PRICE_OFFSET 
    })


if __name__ =="__main__":

    port =int (
    os .environ .get (
    "PORT",
    5000 
    )
    )

    app .run (
    host ="0.0.0.0",
    port =port ,
    debug =False 
    )
