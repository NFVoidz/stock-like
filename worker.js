const WebSocket = require('ws');
const http = require('http');
const key = process.env.ALPACA_API_KEY, secret = process.env.ALPACA_SECRET_KEY;
const state = {connected:false, subscribed:false, events:0, latest:{}, lastEvent:null};
let delay=1000;
function connect(){
 if(!key||!secret)return;
 const ws=new WebSocket('wss://stream.data.alpaca.markets/v2/iex',{handshakeTimeout:15000});
 let alive=true;
 const timer=setInterval(()=>{if(!alive){ws.terminate();return;}if(ws.readyState===1){alive=false;ws.ping();}},20000);
 ws.on('pong',()=>{alive=true});
 ws.on('open',()=>{state.connected=true;ws.send(JSON.stringify({action:'auth',key,secret}));});
 ws.on('message',raw=>{try{for(const m of JSON.parse(raw)){
 if(m.T==='success'&&m.msg==='authenticated')ws.send(JSON.stringify({action:'subscribe',trades:['SPY','QQQ'],quotes:['SPY','QQQ'],bars:['SPY','QQQ']}));
 if(m.T==='subscription'){state.subscribed=true;delay=1000;}
 if(m.S&&['t','q','b'].includes(m.T)){state.events++;state.lastEvent=new Date().toISOString();(state.latest[m.S]??={})[m.T]=m;}
 if(m.T==='error'){console.error('Stream error',m.code);ws.close();}
 }}catch{console.error('Invalid stream message');}});
 ws.on('error',()=>{});
 ws.on('close',()=>{clearInterval(timer);state.connected=state.subscribed=false;setTimeout(connect,delay);delay=Math.min(delay*2,60000);});
}
http.createServer((req,res)=>{res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');if(req.url==='/health'){res.end(JSON.stringify({running:true,configured:!!(key&&secret),connected:state.connected,subscribed:state.subscribed,events:state.events,lastEvent:state.lastEvent}));return;}res.statusCode=404;res.end('{}');}).listen(process.env.PORT||8080);
connect();
