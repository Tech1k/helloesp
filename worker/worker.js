// HelloESP Cloudflare Worker relay (WebSocket + chunked base64 streaming)

const PAGE_CSS = `*{margin:0;padding:0;box-sizing:border-box}:root{--bg:#f8f7f4;--ink:#1a1a1a;--mid:#888;--faint:#ccc}@media(prefers-color-scheme:dark){:root{--bg:#111110;--ink:#e8e6e1;--mid:#8a8a87;--faint:#2a2a28}}body{background:var(--bg);color:var(--ink);font-family:ui-monospace,"SF Mono","Cascadia Mono","Consolas",monospace;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:24px}main{margin:auto 0}main{max-width:480px;text-align:center}h1{font-size:clamp(48px,10vw,72px);font-weight:400;letter-spacing:-0.02em;margin-bottom:20px}p{font-size:13px;color:var(--mid);line-height:1.8;margin-bottom:16px}p.lede{color:var(--ink);font-size:14px;margin-bottom:24px}.note{font-size:11px;color:var(--mid);border-top:1px solid var(--faint);padding-top:20px;margin-top:28px;line-height:1.7}a{color:var(--ink);font-size:11px;letter-spacing:0.1em;text-transform:uppercase;text-underline-offset:3px;display:inline-block;margin:0 8px}.site-name{display:block;font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:var(--mid);text-decoration:none;margin:0 0 8px}.site-name:hover{color:var(--ink)}.status{font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:var(--mid);margin-top:28px}.dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--mid);margin-left:6px;vertical-align:middle;animation:pulse 1.4s ease-in-out infinite}@keyframes pulse{0%,100%{opacity:0.25}50%{opacity:1}}a:focus-visible{outline:2px solid var(--ink);outline-offset:2px;border-radius:2px}.action{color:var(--ink);font-weight:600}.helper{font-size:10px;color:var(--mid);letter-spacing:0.08em;text-transform:uppercase;margin-top:6px}@media(prefers-reduced-motion:reduce){.dot{animation:none}}`;

const CHIP_ICON_PATHS = `<path fill-rule="evenodd" d="M7 5a2 2 0 00-2 2v18a2 2 0 002 2h18a2 2 0 002-2V7a2 2 0 00-2-2H7zm6 7a1 1 0 00-1 1v6a1 1 0 001 1h6a1 1 0 001-1v-6a1 1 0 00-1-1h-6z"/><rect x="7.5" y="1" width="2" height="3" rx=".5"/><rect x="12.5" y="1" width="2" height="3" rx=".5"/><rect x="17.5" y="1" width="2" height="3" rx=".5"/><rect x="22.5" y="1" width="2" height="3" rx=".5"/><rect x="7.5" y="28" width="2" height="3" rx=".5"/><rect x="12.5" y="28" width="2" height="3" rx=".5"/><rect x="17.5" y="28" width="2" height="3" rx=".5"/><rect x="22.5" y="28" width="2" height="3" rx=".5"/><rect x="1" y="7.5" width="3" height="2" rx=".5"/><rect x="1" y="12.5" width="3" height="2" rx=".5"/><rect x="1" y="17.5" width="3" height="2" rx=".5"/><rect x="1" y="22.5" width="3" height="2" rx=".5"/><rect x="28" y="7.5" width="3" height="2" rx=".5"/><rect x="28" y="12.5" width="3" height="2" rx=".5"/><rect x="28" y="17.5" width="3" height="2" rx=".5"/><rect x="28" y="22.5" width="3" height="2" rx=".5"/>`;

const SNAKE_GAME = `<div class="snk" role="group" aria-label="Snake"><div class="snk-h"><span class="snk-h-l">Snake</span><div class="snk-h-r"><span class="snk-stat"><span class="snk-stat-k">Score</span><b id="snk-s">0</b></span><span class="snk-stat snk-stat-pb" id="snk-pb-wrap" hidden><span class="snk-stat-k">Best</span><b id="snk-pb">0</b></span></div></div><div class="snk-board"><canvas id="snk-c" width="280" height="280" aria-hidden="true"></canvas><div class="snk-o" id="snk-o"><div class="snk-o-inner" id="snk-m"><div class="snk-go">Snake</div><div class="snk-cta">tap to play</div><div class="snk-hint"><span class="snk-hint-desk">Arrow keys or WASD</span><span class="snk-hint-touch">Swipe or tap arrows below</span></div></div></div></div><button type="button" class="snk-stop-replay" id="snk-stop-replay" hidden>Stop replay</button><div class="snk-p" aria-hidden="true"><button type="button" data-snk="up">&uarr;</button><button type="button" data-snk="left">&larr;</button><button type="button" data-snk="down">&darr;</button><button type="button" data-snk="right">&rarr;</button></div><div class="snk-lb" id="snk-lb-today-section" hidden><div class="snk-lb-h"><span class="snk-lb-h-t">Today</span><span class="snk-lb-h-line"></span><span class="snk-lb-h-hint">tap &#9654; to watch</span></div><ol class="snk-lb-l" id="snk-lb-today"></ol></div><div class="snk-lb" id="snk-lb-alltime-section" hidden><div class="snk-lb-h"><span class="snk-lb-h-t">All-time</span><span class="snk-lb-h-line"></span><span class="snk-lb-h-hint">tap &#9654; to watch</span></div><ol class="snk-lb-l" id="snk-lb-alltime"></ol></div></div><style>.snk{max-width:280px;margin:32px auto 0;text-align:left}.snk-board{position:relative}.snk-h{display:flex;justify-content:space-between;align-items:baseline;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:var(--mid);margin-bottom:8px}.snk-h-l{font-weight:500;color:var(--ink)}.snk-h-r{display:flex;gap:14px;align-items:baseline}.snk-stat{display:inline-flex;gap:6px;align-items:baseline}.snk-stat-k{font-size:9px;letter-spacing:0.12em;color:var(--mid)}.snk-stat b{color:var(--ink);font-weight:500;font-size:14px;letter-spacing:0.02em;font-variant-numeric:tabular-nums}.snk-stat-pb b{color:var(--mid)}#snk-c{display:block;width:100%;max-width:280px;aspect-ratio:1;background:var(--faint);border:1px solid var(--faint);border-radius:3px;image-rendering:pixelated;touch-action:none;cursor:crosshair;box-shadow:inset 0 0 0 1px rgba(0,0,0,0.06)}.snk-o{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.62);color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;padding:16px;text-align:center;border-radius:3px}.snk-o.hide{display:none}.snk-o-inner{max-width:240px}.snk-o .snk-go{font-size:22px;letter-spacing:-0.01em;text-transform:none;font-weight:400;color:#fff;margin-bottom:6px;line-height:1}.snk-o .snk-go-sc{font-size:36px;font-weight:300;letter-spacing:0;color:#fff;font-variant-numeric:tabular-nums;line-height:1;margin:8px 0 6px}.snk-o .snk-go-pb{font-size:9px;color:#bbb;letter-spacing:0.1em;margin-top:4px}.snk-o .snk-go-sub{font-size:9px;opacity:0.55;letter-spacing:0.12em;margin-top:14px}.snk-o .snk-cta{font-size:11px;color:#fff;opacity:0.85;letter-spacing:0.18em;text-transform:uppercase;margin-top:14px;animation:snk-pulse 2.4s ease-in-out infinite}.snk-o .snk-hint{font-size:9px;color:#fff;opacity:0.45;letter-spacing:0.1em;margin-top:10px;text-transform:uppercase}.snk-hint-touch{display:none}@media(hover:none) and (pointer:coarse){.snk-hint-desk{display:none}.snk-hint-touch{display:inline}}.snk-add-lb{font:inherit;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;padding:7px 14px;background:transparent;color:#fff;border:1px solid #aaa;border-radius:3px;cursor:pointer;margin-top:12px;transition:border-color 0.1s,background 0.1s}.snk-add-lb:hover{border-color:#fff;background:rgba(255,255,255,0.08)}.snk-p{display:none;grid-template-columns:repeat(3,52px);grid-template-rows:repeat(2,52px);gap:4px;justify-content:center;margin:12px auto 0}.snk-p button{background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:20px;border-radius:4px;touch-action:manipulation;cursor:pointer;transition:color 0.1s,border-color 0.1s}.snk-p button:hover,.snk-p button:active{color:var(--ink);border-color:var(--mid)}.snk-p [data-snk=up]{grid-column:2;grid-row:1}.snk-p [data-snk=left]{grid-column:1;grid-row:2}.snk-p [data-snk=down]{grid-column:2;grid-row:2}.snk-p [data-snk=right]{grid-column:3;grid-row:2}@media(hover:none)and(pointer:coarse){.snk-p{display:grid}}.snk-lb{margin-top:18px;font-size:11px;color:var(--mid)}.snk-lb-h{display:flex;align-items:center;gap:8px;margin-bottom:8px}.snk-lb-h-t{text-transform:uppercase;letter-spacing:0.15em;font-size:9px;color:var(--mid);white-space:nowrap}.snk-lb-h-line{flex:1;height:1px;background:var(--faint)}.snk-lb-h-hint{font-size:9px;color:var(--mid);text-transform:none;letter-spacing:0;opacity:0.75;white-space:nowrap;font-style:italic}.snk-lb-l{list-style:none;padding:0;margin:0;font-variant-numeric:tabular-nums}.snk-lb-l li{display:grid;grid-template-columns:20px 42px 1fr auto;gap:8px;padding:3px 4px;align-items:baseline;border-radius:2px;margin-left:-4px;margin-right:-4px}.snk-lb-l .ri{color:var(--mid);text-align:right;font-size:10px;font-weight:500}.snk-lb-l .ii{color:var(--ink);letter-spacing:0.1em;font-size:11px}.snk-lb-l .sc{color:var(--ink);font-weight:500}.snk-lb-l .dt{color:var(--mid);font-size:9px;letter-spacing:0.05em}.snk-lb-l li.r1 .ri{color:#c89b1a}.snk-lb-l li.r2 .ri{color:#9da3ac}.snk-lb-l li.r3 .ri{color:#b87333}.snk-lb-l li.r1{background:rgba(200,155,26,0.07)}.snk-lb-l li.you{outline:1px solid var(--mid)}.snk-lb-l li.you .ii::before{content:'> ';color:var(--mid);letter-spacing:0}.snk-lb-empty{font-size:10px;color:var(--mid);font-style:italic;list-style:none;padding:6px 0;text-align:center}.snk-init-form{display:flex;flex-direction:column;gap:10px;align-items:center;font-family:inherit;text-transform:none;letter-spacing:0;margin-top:12px}.snk-init-headline{font-size:11px;letter-spacing:0.25em;color:#ffd24a;text-transform:uppercase;animation:snk-pulse 1.4s ease-in-out infinite}@keyframes snk-pulse{0%,100%{opacity:0.6}50%{opacity:1}}.snk-init-form input{font:inherit;font-size:24px;letter-spacing:0.5em;text-align:center;text-transform:uppercase;width:120px;padding:8px 6px 8px 12px;background:transparent;border:1px solid #888;color:#fff;border-radius:3px;outline:none}.snk-init-form input:focus{border-color:#fff}.snk-init-form button{font:inherit;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;padding:8px 18px;background:#fff;color:#000;border:none;border-radius:3px;cursor:pointer;transition:background 0.1s}.snk-init-form button:hover{background:#e8e6e1}.snk-init-form button[disabled]{opacity:0.5;cursor:default}.snk-init-err{font-size:10px;color:#ff7a7a;letter-spacing:0.05em;text-transform:none;min-height:14px;text-align:center;font-family:inherit;margin-top:-2px}.snk-init-form.snk-err input{border-color:#ff7a7a}.snk-init-form.snk-shake{animation:snk-shake 0.32s ease-in-out}@keyframes snk-shake{0%,100%{transform:translateX(0)}25%{transform:translateX(-5px)}75%{transform:translateX(5px)}}.snk-init-skip{font-size:9px;opacity:0.55;letter-spacing:0.12em;margin-top:2px}@media(prefers-reduced-motion:reduce){.snk-init-headline{animation:none;opacity:0.9}.snk-o .snk-cta{animation:none}.snk-init-form.snk-shake{animation:none}}.snk-lb-l li{grid-template-columns:20px 42px 1fr auto auto}.snk-watch{background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:9px;padding:2px 6px;cursor:pointer;border-radius:2px;line-height:1;transition:color 0.1s,border-color 0.1s}.snk-watch:hover{color:var(--ink);border-color:var(--mid)}.snk-watch.invisible{visibility:hidden;pointer-events:none}.snk-stop-replay{display:block;margin:8px auto 0;background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;padding:4px 12px;cursor:pointer;border-radius:2px;transition:color 0.1s,border-color 0.1s}.snk-stop-replay[hidden]{display:none}.snk-stop-replay:hover{color:var(--ink);border-color:var(--mid)}</style><script>(function(){var c=document.getElementById('snk-c'),ov=document.getElementById('snk-o'),mg=document.getElementById('snk-m'),sc=document.getElementById('snk-s');if(!c)return;var pbWrap=document.getElementById('snk-pb-wrap'),pbEl=document.getElementById('snk-pb');var cx=c.getContext('2d'),CELL=14,COLS=20,ROWS=20,state='idle',sn,dr,nd,fd,score,ls,ms,stepN,moves,gameId,seed,rngState,showingInit=false,lastTodayRank=null,lastAlltimeRank=null,personalBest=0,pendingSubmit=null,demoTimer=null;var todayEl=document.getElementById('snk-lb-today-section'),todayList=document.getElementById('snk-lb-today'),alltimeEl=document.getElementById('snk-lb-alltime-section'),alltimeList=document.getElementById('snk-lb-alltime'),todayBoard=[],alltimeBoard=[];var API=(location.host==='helloesp.com'||location.host==='www.helloesp.com')?'':'https://helloesp.com';try{var pb=parseInt(localStorage.getItem('snk-pb')||'0',10);if(pb>0){personalBest=pb;pbEl.textContent=pb;pbWrap.hidden=false;}}catch(e){}function esc(s){return String(s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}function fmtDate(t){if(!t)return '';try{return new Date(t*1000).toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'});}catch(e){return '';}}function fmtIso(t){if(!t)return '';try{return new Date(t*1000).toISOString();}catch(e){return '';}}function renderBoard(listEl,sectionEl,data,isAlltime){if(!listEl||!sectionEl)return;if(!data||!data.length){listEl.innerHTML='<li class="snk-lb-empty">No scores yet. Be first.</li>';sectionEl.hidden=false;return;}var youRank=isAlltime?lastAlltimeRank:lastTodayRank;var html='';for(var i=0;i<data.length;i++){var e=data[i];var rankCls=i<3?' r'+(i+1):'';var youCls=(youRank===i+1)?' you':'';html+='<li class="'+(rankCls+youCls).trim()+'"><span class="ri">'+(i+1)+'</span><span class="ii">'+esc(e.i)+'</span><span class="sc">'+e.s+'</span><span class="dt" title="'+esc(fmtIso(e.t))+'">'+esc(fmtDate(e.t))+'</span><button class="snk-watch'+(e.g?'':' invisible')+'" data-g="'+(e.g?esc(e.g):'')+'" aria-label="Watch replay" title="Watch replay" '+(e.g?'':'tabindex="-1" aria-hidden="true"')+'>&#9654;</button></li>';}listEl.innerHTML=html;sectionEl.hidden=false;}function renderBoth(){renderBoard(todayList,todayEl,todayBoard,false);renderBoard(alltimeList,alltimeEl,alltimeBoard,true);}function fetchBoth(){Promise.all([fetch(API+'/snake/daily/leaderboard',{cache:'no-cache'}).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;}),fetch(API+'/snake/leaderboard',{cache:'no-cache'}).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;})]).then(function(results){var todayResp=results[0],altResp=results[1];todayBoard=(todayResp&&Array.isArray(todayResp.leaderboard))?todayResp.leaderboard:[];alltimeBoard=Array.isArray(altResp)?altResp:[];renderBoth();});}function requestSeed(cb){fetch(API+'/snake/seed',{cache:'no-store'}).then(function(r){return r.ok?r.json():null;}).then(function(d){if(d&&typeof d.seed==='number'&&typeof d.gameId==='string'){seed=d.seed;gameId=d.gameId;}cb();}).catch(function(){cb();});}function rng32(s){s=(s+0x6D2B79F5)|0;var t=s;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return [(((t^(t>>>14))>>>0)/4294967296),s];}function sf(){if(seed!=null){for(var i=0;i<200;i++){var r1=rng32(rngState);rngState=r1[1];var r2=rng32(rngState);rngState=r2[1];var x=Math.floor(r1[0]*COLS),y=Math.floor(r2[0]*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}else{for(var k=0;k<400;k++){var x=Math.floor(Math.random()*COLS),y=Math.floor(Math.random()*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}}function reset(){sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};nd=dr;score=0;ms=140;stepN=0;moves=[];rngState=(seed!=null?seed:0)|0;sf();sc.textContent=0;draw();}function step(){if(nd.x!==-dr.x||nd.y!==-dr.y)dr=nd;var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};stepN++;if(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS)return over();if(sn.some(function(s){return s.x===h.x&&s.y===h.y}))return over();sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){score+=10;sc.textContent=score;sf();if(ms>65)ms-=3;}else sn.pop();draw();}function draw(){cx.clearRect(0,0,c.width,c.height);cx.fillStyle=(state==='demo')?'#c97326':'#e67e22';cx.fillRect(fd.x*CELL+2,fd.y*CELL+2,CELL-4,CELL-4);cx.fillStyle=(state==='demo')?'#5a8db9':'#2686e6';for(var i=0;i<sn.length;i++){var s=sn[i];cx.fillRect(s.x*CELL+1,s.y*CELL+1,CELL-2,CELL-2);}}function loop(t){if(state!=='playing')return;if(!ls)ls=t;if(t-ls>=ms){ls=t;step();}if(state==='playing')requestAnimationFrame(loop);}function startGame(){if(state==='playing')return;stopDemo();reset();state='playing';ls=0;ov.classList.add('hide');requestAnimationFrame(loop);}function play(){pendingSubmit=null;lastAlltimeRank=null;lastTodayRank=null;if(state==='paused'){state='playing';ls=0;ov.classList.add('hide');requestAnimationFrame(loop);return;}if(state==='over'){seed=null;gameId=null;}if(seed==null){requestSeed(startGame);return;}startGame();}function pause(){if(state!=='playing')return;state='paused';mg.innerHTML='<div class="snk-go">Paused</div><div class="snk-go-sub">tap to resume</div>';ov.classList.remove('hide');}function updatePB(){if(score>personalBest){personalBest=score;try{localStorage.setItem('snk-pb',String(score));}catch(e){}pbEl.textContent=score;pbWrap.hidden=false;}}function over(){state='over';updatePB();var base=score>=10&&gameId!=null;var qT=base&&(todayBoard.length<10||score>(todayBoard[todayBoard.length-1].s||0));var qA=base&&(alltimeBoard.length<10||score>(alltimeBoard[alltimeBoard.length-1].s||0));if(qT||qA){pendingSubmit={score:score,gameId:gameId,moves:moves.slice(),seed:seed};showInitials(false);}else{pendingSubmit=null;showOver();}}function showOver(){showingInit=false;var pbLine=personalBest>0?'<div class="snk-go-pb">Your best &bull; '+personalBest+'</div>':'';var altLine=lastAlltimeRank?'<div class="snk-go-pb" style="color:#ffd24a;">All-time rank &bull; #'+lastAlltimeRank+'</div>':'';var addBtn=pendingSubmit?'<button type="button" class="snk-add-lb" id="snk-add-lb">Add to leaderboard</button>':'';mg.innerHTML='<div class="snk-go">Game over</div><div class="snk-go-sc">'+score+'</div>'+pbLine+altLine+addBtn+'<div class="snk-go-sub">tap canvas to play again</div>';ov.classList.remove('hide');if(pendingSubmit){var addB=document.getElementById('snk-add-lb');if(addB)addB.addEventListener('click',function(e){e.stopPropagation();showInitials(true);});}}function showInitials(isReopen){showingInit=true;var sChk=pendingSubmit?pendingSubmit.score:score;var altQ=alltimeBoard.length<10||sChk>(alltimeBoard[alltimeBoard.length-1].s||0);var head=isReopen?'Add your initials':(altQ?'New high score':'New daily high score');var displayScore=pendingSubmit?pendingSubmit.score:score;mg.innerHTML='<div class="snk-init-headline">'+head+'</div><div class="snk-go-sc">'+displayScore+'</div><form class="snk-init-form" id="snk-init-f"><input id="snk-init-i" maxlength="3" pattern="[A-Za-z0-9]{3}" placeholder="AAA" autocomplete="off" autocapitalize="characters" inputmode="latin" required><button type="submit">Submit</button><div class="snk-init-err" id="snk-init-err"></div><span class="snk-init-skip">tap outside to skip</span></form>';ov.classList.remove('hide');var f=document.getElementById('snk-init-f'),inp=document.getElementById('snk-init-i');setTimeout(function(){if(inp)inp.focus();},20);f.addEventListener('submit',function(e){e.preventDefault();var v=(inp.value||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,3);if(v.length!==3){inp.focus();return;}submit(v);});}function submit(initials){if(!pendingSubmit)return;var btn=document.querySelector('#snk-init-f button'),inp=document.getElementById('snk-init-i'),err=document.getElementById('snk-init-err'),f=document.getElementById('snk-init-f');if(btn){btn.disabled=true;btn.textContent='...';}if(inp)inp.disabled=true;if(err)err.textContent='';if(f)f.classList.remove('snk-err','snk-shake');var payload={gameId:pendingSubmit.gameId,initials:initials,score:pendingSubmit.score,moves:pendingSubmit.moves};fetch(API+'/snake/score',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}).then(function(r){return r.json().catch(function(){return null;}).then(function(j){return {ok:r.ok,body:j};});}).then(function(res){if(res.ok&&res.body&&res.body.ok){if(res.body.today&&Array.isArray(res.body.today.board)){todayBoard=res.body.today.board;lastTodayRank=res.body.today.rank||null;}if(res.body.alltime&&Array.isArray(res.body.alltime.board)){alltimeBoard=res.body.alltime.board;lastAlltimeRank=(typeof res.body.alltime.rank==='number')?res.body.alltime.rank:null;}renderBoth();pendingSubmit=null;gameId=null;seed=null;showOver();}else{if(btn){btn.disabled=false;btn.textContent='Submit';}if(inp){inp.disabled=false;inp.value='';inp.focus();}var msg=(res.body&&res.body.error)||'submission failed';if(err)err.textContent=msg;if(f){f.classList.add('snk-err','snk-shake');setTimeout(function(){if(f)f.classList.remove('snk-shake');},340);}var fatalRe=/expired|not found|score does not match/i;if(res.body&&res.body.error&&fatalRe.test(res.body.error)){pendingSubmit=null;if(btn)btn.disabled=true;if(inp)inp.disabled=true;}}}).catch(function(){if(btn){btn.disabled=false;btn.textContent='Submit';}if(inp)inp.disabled=false;if(err)err.textContent='connection failed, try again';if(f){f.classList.add('snk-shake');setTimeout(function(){if(f)f.classList.remove('snk-shake');},340);}});}function setDir(x,y){if(state!=='playing')return;if(x===-dr.x&&y===-dr.y)return;if(nd.x===x&&nd.y===y)return;nd={x:x,y:y};var ch=x===0?(y<0?'U':'D'):(x<0?'L':'R');moves.push(stepN+ch);}function aiPick(){var head=sn[0],dx=fd.x-head.x,dy=fd.y-head.y,prefs=[];if(Math.abs(dx)>=Math.abs(dy)){if(dx>0)prefs.push([1,0]);else if(dx<0)prefs.push([-1,0]);if(dy>0)prefs.push([0,1]);else if(dy<0)prefs.push([0,-1]);}else{if(dy>0)prefs.push([0,1]);else if(dy<0)prefs.push([0,-1]);if(dx>0)prefs.push([1,0]);else if(dx<0)prefs.push([-1,0]);}var all=[[1,0],[-1,0],[0,1],[0,-1]];for(var i=0;i<all.length;i++){var found=false;for(var j=0;j<prefs.length;j++)if(prefs[j][0]===all[i][0]&&prefs[j][1]===all[i][1]){found=true;break;}if(!found)prefs.push(all[i]);}for(var k=0;k<prefs.length;k++){var mv=prefs[k];if(mv[0]===-dr.x&&mv[1]===-dr.y)continue;var nx=head.x+mv[0],ny=head.y+mv[1];if(nx<0||nx>=COLS||ny<0||ny>=ROWS)continue;var willGrow=(nx===fd.x&&ny===fd.y),hit=false;for(var l=0;l<sn.length-(willGrow?0:1);l++)if(sn[l].x===nx&&sn[l].y===ny){hit=true;break;}if(!hit)return mv;}return null;}function demoSpawnFood(){for(var k=0;k<400;k++){var x=Math.floor(Math.random()*COLS),y=Math.floor(Math.random()*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}function demoStart(){if(state==='playing'||state==='paused'||state==='over')return;state='demo';sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};demoSpawnFood();draw();demoTick();}function demoTick(){if(state!=='demo'){demoTimer=null;return;}var pick=aiPick();if(!pick){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},800);return;}dr={x:pick[0],y:pick[1]};var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};if(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS||sn.some(function(s){return s.x===h.x&&s.y===h.y})){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},800);return;}sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){demoSpawnFood();if(sn.length>=COLS*ROWS-3){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},1200);draw();return;}}else sn.pop();draw();demoTimer=setTimeout(demoTick,170);}function stopDemo(){if(demoTimer){clearTimeout(demoTimer);demoTimer=null;}}var K={ArrowUp:[0,-1],ArrowDown:[0,1],ArrowLeft:[-1,0],ArrowRight:[1,0],w:[0,-1],s:[0,1],a:[-1,0],d:[1,0],W:[0,-1],S:[0,1],A:[-1,0],D:[1,0]};document.addEventListener('keydown',function(e){if(showingInit)return;if(!K[e.key])return;e.preventDefault();if(state!=='playing')play();else setDir(K[e.key][0],K[e.key][1]);});var ts=null;c.addEventListener('touchstart',function(e){ts={x:e.touches[0].clientX,y:e.touches[0].clientY};},{passive:true});c.addEventListener('touchmove',function(e){if(ts&&state==='playing')e.preventDefault();},{passive:false});c.addEventListener('touchend',function(e){if(!ts)return;var t=e.changedTouches[0],dx=t.clientX-ts.x,dy=t.clientY-ts.y;if(Math.abs(dx)<20&&Math.abs(dy)<20){ts=null;return;}if(state==='playing'){if(Math.abs(dx)>Math.abs(dy))setDir(dx>0?1:-1,0);else setDir(0,dy>0?1:-1);}else if(!showingInit){play();}ts=null;});document.querySelectorAll('.snk-p button').forEach(function(b){b.addEventListener('click',function(){if(showingInit)return;if(state!=='playing'){play();return;}var d=b.getAttribute('data-snk');if(d==='up')setDir(0,-1);else if(d==='down')setDir(0,1);else if(d==='left')setDir(-1,0);else if(d==='right')setDir(1,0);});});ov.addEventListener('click',function(e){if(e.target.closest&&e.target.closest('#snk-init-f'))return;if(e.target.closest&&e.target.closest('#snk-add-lb'))return;if(showingInit){showOver();return;}if(state!=='playing')play();});var prefersReducedMotion=function(){try{return window.matchMedia&&window.matchMedia('(prefers-reduced-motion: reduce)').matches;}catch(e){return false;}};document.addEventListener('visibilitychange',function(){if(document.hidden){if(state==='playing')pause();if(state==='demo')stopDemo();}else{if((state==='idle'||state==='demo')&&!prefersReducedMotion())demoStart();}});function fetchAndPlayReplay(gid){if(state==='playing'||state==='paused')return;fetch(API+'/snake/replay?gameId='+encodeURIComponent(gid),{cache:'force-cache'}).then(function(r){return r.ok?r.json():null;}).then(function(d){if(!d||typeof d.seed!=='number'||!Array.isArray(d.moves))return;startReplay(d);}).catch(function(){});}function startReplay(rp){stopDemo();state='replay';pendingSubmit=null;seed=rp.seed|0;rngState=seed;sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};nd=dr;score=0;ms=140;stepN=0;sf();sc.textContent='0';ov.classList.add('hide');var stopBtn=document.getElementById('snk-stop-replay');if(stopBtn)stopBtn.hidden=false;draw();var mIdx=0,rm=rp.moves,DIRS={U:[0,-1],D:[0,1],L:[-1,0],R:[1,0]};function rstep(){if(state!=='replay')return;while(mIdx<rm.length){var m=String(rm[mIdx]),ch=m.charAt(m.length-1),n=parseInt(m,10);if(n>stepN)break;if(n===stepN){var dir=DIRS[ch];if(dir&&(dir[0]!==-dr.x||dir[1]!==-dr.y))nd={x:dir[0],y:dir[1]};}mIdx++;}if(nd.x!==-dr.x||nd.y!==-dr.y)dr=nd;var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};stepN++;var dead=(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS)||sn.some(function(s){return s.x===h.x&&s.y===h.y;});if(dead){state='idle';if(stopBtn)stopBtn.hidden=true;mg.innerHTML='<div class="snk-go">Replay done</div><div class="snk-go-sc">'+score+'</div><div class="snk-go-sub">tap canvas to play</div>';ov.classList.remove('hide');return;}sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){score+=10;sc.textContent=score;sf();if(ms>65)ms-=3;}else sn.pop();draw();setTimeout(rstep,ms);}setTimeout(rstep,ms);}function stopReplay(){if(state!=='replay')return;state='idle';var sb=document.getElementById('snk-stop-replay');if(sb)sb.hidden=true;mg.innerHTML='<div class="snk-go">Replay stopped</div><div class="snk-go-sub">tap canvas to play</div>';ov.classList.remove('hide');}var stopBtnEl=document.getElementById('snk-stop-replay');if(stopBtnEl)stopBtnEl.addEventListener('click',stopReplay);var watchHandler=function(ev){var b=ev.target.closest&&ev.target.closest('.snk-watch');if(!b||b.classList.contains('invisible'))return;var g=b.getAttribute('data-g');if(g)fetchAndPlayReplay(g);};if(todayList)todayList.addEventListener('click',watchHandler);if(alltimeList)alltimeList.addEventListener('click',watchHandler);fetchBoth();setTimeout(function(){if(state==='idle'&&!prefersReducedMotion())demoStart();},400);})();</script>`;

const FAVICON = `<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="#2686e6">${CHIP_ICON_PATHS}</svg>`).replace(/'/g, '%27').replace(/"/g, '%22')}">`;

// The header "HelloESP" breadcrumb used on every Worker-served page
const SITE_NAME_LINK = `<a href="/" class="site-name"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" width="12" height="12" fill="#2686e6" style="vertical-align:-2px;margin-right:5px;">${CHIP_ICON_PATHS}</svg>HelloESP</a>`;

// Maintenance pages set body[data-until=<unix-ms>] so we can schedule a
// single reload at the end of the window instead of polling every 3s.
// Offline/timeout pages don't know when the ESP recovers, so they fall
// back to polling /ping. busy() pauses the polling path while snake is
// being played; the scheduled-reload path doesn't need it because it
// only fires once at a known time.
const RETRY_JS = `<script>(function(){var u=document.body.getAttribute('data-until');if(u){var d=parseInt(u,10)-Date.now()+2000;if(d>0)setTimeout(function(){location.reload()},d);return;}function busy(){var ov=document.getElementById('snk-o');if(!ov)return false;if(ov.classList.contains('hide'))return true;if(document.getElementById('snk-init-f'))return true;return false;}setInterval(function(){if(document.visibilityState!=='visible')return;if(busy())return;fetch('/ping?_='+Date.now(),{cache:'no-store'}).then(function(r){if(r.ok)location.reload()}).catch(function(){})},3000)})();</script>`;

const OFFLINE_HTML = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Offline / HelloESP</title><style>${PAGE_CSS}</style></head><body>${SITE_NAME_LINK}<main><h1>Offline</h1><p class="lede">The ESP32 serving this site isn't connected right now.</p><p>It might be rebooting, out of WiFi range, or unplugged. It'll come back on its own.</p><p><a href="/" class="action">Retry</a><a href="https://github.com/Tech1k/helloesp" target="_blank" rel="noopener">GitHub</a></p><p class="status" role="status" aria-live="polite">Reconnecting<span class="dot" aria-hidden="true"></span></p><p class="helper">Auto-retrying every 3 seconds</p>${SNAKE_GAME}<p class="note">HelloESP runs entirely on an ESP32. When the chip is unreachable, Cloudflare serves this page instead.</p></main>${RETRY_JS}</body></html>`;

const TIMEOUT_HTML = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Timeout / HelloESP</title><style>${PAGE_CSS}</style></head><body>${SITE_NAME_LINK}<main><h1>Timeout</h1><p class="lede">The ESP32 got your request but didn't answer in time.</p><p>Probably busy handling something else. Try again in a moment.</p><p><a href="/" class="action">Retry</a><a href="https://github.com/Tech1k/helloesp" target="_blank" rel="noopener">GitHub</a></p><p class="status" role="status" aria-live="polite">Retrying<span class="dot" aria-hidden="true"></span></p><p class="helper">Auto-retrying every 3 seconds</p>${SNAKE_GAME}<p class="note">HelloESP runs entirely on an ESP32. If a request takes over 30 seconds, Cloudflare shows this page.</p></main>${RETRY_JS}</body></html>`;

const SEC_HEADERS = {
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'strict-origin-when-cross-origin'
};

function applySecHeaders(h) {
  for (const [k, v] of Object.entries(SEC_HEADERS)) h.set(k, v);
  return h;
}

function offlineResponse() {
  return new Response(OFFLINE_HTML, { status: 502, headers: { 'Content-Type': 'text/html', 'Cache-Control': 'no-store', ...SEC_HEADERS } });
}

function timeoutResponse() {
  return new Response(TIMEOUT_HTML, { status: 504, headers: { 'Content-Type': 'text/html', 'Cache-Control': 'no-store', ...SEC_HEADERS } });
}

function escapeHtml(s) {
  // `/` is also escaped so this remains safe even inside a `<script>` block,
  // where `</script>` would otherwise terminate the script context.
  return String(s).replace(/[&<>"'\/]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#47;'}[c]));
}

// Parse "14 days, 3 hours, 22 minutes, 45 seconds" into a compact "14d 3h" / "3h 22m" / "22m 45s" string
function formatUptime(str) {
  if (!str) return '';
  const m = String(str).match(/(\d+)\s*days?[,\s]+(\d+)\s*hours?[,\s]+(\d+)\s*minutes?[,\s]+(\d+)\s*seconds?/);
  if (!m) return str;
  const d = +m[1], h = +m[2], mn = +m[3], s = +m[4];
  if (d > 0)  return `up ${d}d ${h}h`;
  if (h > 0)  return `up ${h}h ${mn}m`;
  if (mn > 0) return `up ${mn}m ${s}s`;
  return `up ${s}s`;
}

function maintenanceResponse(until, message) {
  const remainingMs = Math.max(0, until - Date.now());
  const safeMsg = message ? escapeHtml(String(message).slice(0, 200)) : '';
  const lede = safeMsg || "The ESP is down for maintenance.";
  let etaLine;
  let retryAfter;
  if (remainingMs < 60000) {
    etaLine = 'Back shortly.';
    retryAfter = 30;
  } else {
    const mins = Math.ceil(remainingMs / 60000);
    etaLine = `Back in about ${mins} ${mins === 1 ? 'minute' : 'minutes'}.`;
    retryAfter = Math.min(3600, mins * 60);
  }
  const html = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Maintenance / HelloESP</title><style>${PAGE_CSS}</style></head><body data-until="${until}">${SITE_NAME_LINK}<main><h1>Maintenance</h1><p class="lede">${lede}</p><p>${etaLine}</p><p><a href="/" class="action">Retry</a><a href="https://github.com/Tech1k/helloesp" target="_blank" rel="noopener">GitHub</a></p><p class="status" role="status" aria-live="polite">Checking<span class="dot" aria-hidden="true"></span></p>${SNAKE_GAME}<p class="note">HelloESP runs entirely on an ESP32. Planned work is in progress. The page will refresh automatically when the site is back.</p></main>${RETRY_JS}</body></html>`;
  return new Response(html, {
    status: 503,
    headers: {
      'Content-Type': 'text/html',
      'Cache-Control': 'no-store',
      'Retry-After': String(retryAfter),
      ...SEC_HEADERS
    }
  });
}

function b64ToBytes(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

// Constant-time string compare; avoids leaking secret length/content via
// early-exit timing when comparing provided creds against WORKER_SECRET.
function timingSafeEqualStr(a, b) {
  const enc = new TextEncoder();
  const ab = enc.encode(String(a ?? ''));
  const bb = enc.encode(String(b ?? ''));
  const len = Math.max(ab.length, bb.length);
  let diff = ab.length ^ bb.length;
  for (let i = 0; i < len; i++) {
    const av = i < ab.length ? ab[i] : 0;
    const bv = i < bb.length ? bb[i] : 0;
    diff |= av ^ bv;
  }
  return diff === 0;
}

const MAX_BODY = 8192;
const RATE_LIMIT_WINDOW = 60000; // 1 min
const RATE_LIMIT_MAX = 60;       // per IP per window
const SSE_MAX_CLIENTS = 500;     // cap concurrent SSE connections so a flood can't exhaust DO memory
const SSE_MAX_PAYLOAD = 64 * 1024; // per-event byte cap so one fat event * 500 clients can't OOM the DO
// Cap per-game move count for Snake replay verification. Used as both a
// step-index ceiling and an array-length cap. Mirrors `moves.length` checks
// in the inline client (SNAKE_GAME above and data/404.html). Keep these in
// sync; a tighter server cap would silently reject honest games.
const SNAKE_MAX_STEPS = 5000;

// Snake leaderboard 3-letter initials blocklist. ROT13'd in source so this
// file reads clean for casual visitors (educational context). NOT security:
// the input space is only 46k combos so anyone determined can rebuild this
// from hashes or enumeration. The point is just that the source doesn't
// display slurs verbatim. Decoded once at module load.
const BLOCKED_INITIALS = new Set(
  // profanity (rot13)
  ['NEF','NFF','OWF','PPX','PBX','PHP','PHZ','QVP','QVX','QVK','SPX',
   'SHP','SHX','SHK','UBR','WVM','WMZ','XBX','CBB','FRK','FYG','GVG',
   'GJN','GJG','JGS',
   // slurs (rot13)
   'PUX','QLX','SNT','STF','STG','TBX','WNC','WRJ','XXX','XLX','ATN',
   'ATE','AVT','AVD','CNX','FCP','JBC',
   // self-harm / harassment shorthand (rot13)
   'QVR','XZF','XLF']
    .map(s => s.replace(/[A-Z]/g, c => String.fromCharCode((c.charCodeAt(0) - 65 + 13) % 26 + 65)))
);

// Weather proxy; Denver, CO is broad enough to be non-identifying (~3M metro pop)
const WEATHER_LAT = 39.74;
const WEATHER_LON = -104.99;
const WEATHER_LOCATION = 'Denver, CO';
const WEATHER_REFRESH_MS = 3600000; // 1 hour
const WEATHER_STALE_MS   = 7200000; // after 2h with no successful refresh, stop sending outdoor data

// Email backup bundle limits. The Worker chunks the final bundle across however many emails
// are needed (see BACKUP_PART_SIZE). These ceilings are runaway protection only.
const BACKUP_MAX_B64       = 80 * 1024 * 1024; // ~60 MB raw bytes once decoded
const BACKUP_MAX_CHUNKS    = 25000;             // sanity cap on per-session WS frames
const BACKUP_SESSION_IDLE  = 15 * 60 * 1000;    // drop sessions idle > 15 min
const BACKUP_PART_SIZE     = 7 * 1024 * 1024;   // raw-byte slice per email (safely < SMTP2GO 10 MB rec.)
const BACKUP_PART_DELAY_MS = 2000;              // pause between multipart sends

export class EspRelay {
  constructor(state, env) {
    this.state = state;
    this.env = env || {};
    this.espSocket = null;
    this.pendingRequests = new Map();
    this.activeResponses = new Map();
    this.requestId = 0;
    this.currentStreamId = null;
    this.lastActivity = 0;
    this.rateLimits = new Map();
    this.wsAuthFails = new Map();
    this.lastEmailAt = 0;
    this.hmacAuthenticated = false;
    this.maintenanceUntil = 0;
    this.maintenanceMessage = '';
    this.sseClients = new Set();
    this.lastStats = null;  // JSON string of the most recent ESP stats push
    this.lastStatsAt = 0;   // epoch ms when lastStats was set; used to detect staleness for badges
    this.lastWeather = null; // cached outdoor weather object
    this.lastAirQuality = null; // cached outdoor air-quality object (PM2.5, US AQI)
    this.deadmanAlertSent = false; // so we don't spam when offline persists past 24h
    this.backupSessions = new Map(); // seq -> { startedAt, meta, files[], currentFile, totalB64, aborted }
    this.lastBackupAt = 0;
    this.lastBackupDate = '';
    this.lastBackupFailureEmailAt = 0;
    this.lastBackupMissedEmailAt = 0;
    this.firstSeenAt = 0;  // DO construction time, used as fallback floor for missed-backup alert
    this.state.blockConcurrencyWhile(async () => {
      const u = await state.storage.get('maintenanceUntil');
      const m = await state.storage.get('maintenanceMessage');
      const w = await state.storage.get('lastWeather');
      const aq = await state.storage.get('lastAirQuality');
      const dm = await state.storage.get('deadmanAlertSent');
      const lba = await state.storage.get('lastBackupAt');
      const lbd = await state.storage.get('lastBackupDate');
      const lact = await state.storage.get('lastActivity');
      const fseen = await state.storage.get('firstSeenAt');
      if (typeof u === 'number') this.maintenanceUntil = u;
      if (typeof m === 'string') this.maintenanceMessage = m;
      if (w && typeof w === 'object') this.lastWeather = w;
      if (aq && typeof aq === 'object') this.lastAirQuality = aq;
      if (typeof dm === 'boolean') this.deadmanAlertSent = dm;
      if (typeof lba === 'number') this.lastBackupAt = lba;
      if (typeof lbd === 'string') this.lastBackupDate = lbd;
      // Restoring lastActivity after isolate eviction: without this, a truly-
      // dead device's deadman alert can't fire because the `lastActivity > 0`
      // guard rejects the default-zero value, AND a recovery email could fire
      // spuriously when the ESP reconnects to a fresh isolate.
      if (typeof lact === 'number') this.lastActivity = lact;
      // First-seen tracking: stamped once on initial DO creation, used as a
      // fallback timestamp for the missed-backup alert when no successful
      // backup has ever happened. Without this, a fresh deploy that never
      // gets a successful backup would never alert.
      if (typeof fseen === 'number') {
        this.firstSeenAt = fseen;
      } else {
        this.firstSeenAt = Date.now();
        await state.storage.put('firstSeenAt', this.firstSeenAt);
      }
    });
    this._ensureAlarm(30000);
  }

  // Only schedule a new alarm if none is set or the existing one is further out
  // than the requested window. Otherwise we keep pushing the alarm into the
  // future on every SSE connect / constructor call, which starves the deadman.
  async _ensureAlarm(msFromNow) {
    const target = Date.now() + msFromNow;
    const existing = await this.state.storage.getAlarm();
    if (existing == null || existing > target) {
      await this.state.storage.setAlarm(target);
    }
  }

  // SMTP2GO send wrapper. Returns the Response when SMTP2GO is configured
  // (so callers can check .ok), or null when keys are missing.
  async _sendEmail({ subject, text_body, attachments }) {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return null;
    const payload = {
      sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
      to: [env.NOTIFY_EMAIL],
      subject,
      text_body
    };
    if (attachments) payload.attachments = attachments;
    return fetch('https://api.smtp2go.com/v3/email/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
      body: JSON.stringify(payload)
    });
  }

  // Per-IP rate limit check. Returns null when allowed, or a 429 Response
  // when exceeded. Non-relay endpoints (/snake/seed, /snake/score, etc.)
  // call this directly so they aren't a back door around the relay-path
  // limit at the bottom of fetch().
  _enforceRateLimit(clientIP) {
    const now = Date.now();
    let rl = this.rateLimits.get(clientIP);
    if (!rl || now > rl.resetAt) {
      rl = { count: 0, resetAt: now + RATE_LIMIT_WINDOW };
      this.rateLimits.set(clientIP, rl);
    }
    rl.count++;
    if (rl.count > RATE_LIMIT_MAX) {
      return new Response('Rate limit exceeded', {
        status: 429,
        headers: { 'Content-Type': 'text/plain', 'Retry-After': '60', ...SEC_HEADERS }
      });
    }
    if (this.rateLimits.size > 500) {
      for (const [k, v] of this.rateLimits) {
        if (v.resetAt < now) this.rateLimits.delete(k);
      }
    }
    return null;
  }

  async maybeSendDeadmanAlert() {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    const now = Date.now();
    // DEADMAN_HOURS env var overrides default; typical home has near-zero ISP outages >6h
    const hoursCfg = parseFloat(env.DEADMAN_HOURS);
    const DEAD_HOURS = (hoursCfg > 0 && hoursCfg < 720) ? hoursCfg : 6;
    const DEAD_MS = DEAD_HOURS * 3600000;
    const BACK_MS = 300000;   // 5 minutes of fresh activity = considered back
    const elapsed = now - this.lastActivity;

    // device has been silent for >DEAD_HOURS and we haven't alerted yet
    if (this.lastActivity > 0 && elapsed > DEAD_MS && !this.deadmanAlertSent) {
      this.deadmanAlertSent = true;
      await this.state.storage.put('deadmanAlertSent', true);
      const hours = Math.floor(elapsed / 3600000);
      const lastSeen = new Date(this.lastActivity).toISOString();
      const body = `HelloESP has been unreachable for ${hours} hours.\n\nLast heartbeat: ${lastSeen}\n\nThe device may be offline, rebooting into a loop, or has lost WiFi.\nThis is an automated dead-man's-switch alert; you won't get another until it recovers and goes silent again.`;
      try {
        await this._sendEmail({
          subject: `HelloESP unreachable (${hours}h)`,
          text_body: body
        });
      } catch (e) { console.error('deadman email failed:', e && e.message); }
      return;
    }

    // device came back; clear the flag and send a recovery notification
    if (this.deadmanAlertSent && elapsed < BACK_MS) {
      this.deadmanAlertSent = false;
      await this.state.storage.put('deadmanAlertSent', false);
      const body = `HelloESP is back online.\n\nFirst fresh heartbeat: ${new Date(this.lastActivity).toISOString()}\n\nThis is a dead-man's-switch recovery notification.`;
      try {
        await this._sendEmail({ subject: 'HelloESP recovered', text_body: body });
      } catch (e) { console.error('deadman-recovered email failed:', e && e.message); }
    }
  }

  async refreshAirQuality() {
    try {
      const url = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=${WEATHER_LAT}&longitude=${WEATHER_LON}&current=us_aqi,pm2_5,carbon_dioxide,uv_index`;
      const res = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!res.ok) { console.error('air quality http', res.status); return; }
      const data = await res.json();
      if (!data || !data.current) return;
      const c = data.current;
      this.lastAirQuality = {
        us_aqi:        c.us_aqi,
        pm2_5:         c.pm2_5,
        co2_ppm:       c.carbon_dioxide,
        uv_index:      c.uv_index,
        fetched_at:    Date.now()
      };
      await this.state.storage.put('lastAirQuality', this.lastAirQuality);
    } catch (e) {
      console.error('air quality fetch failed:', e && e.message);
    }
  }

  async refreshWeather() {
    try {
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${WEATHER_LAT}&longitude=${WEATHER_LON}&current=temperature_2m,apparent_temperature,relative_humidity_2m,dew_point_2m,weather_code,wind_speed_10m,wind_direction_10m,surface_pressure,is_day&temperature_unit=fahrenheit&wind_speed_unit=mph`;
      const res = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!res.ok) { console.error('weather http', res.status); return; }
      const data = await res.json();
      if (!data || !data.current) return;
      const c = data.current;
      // Capture previous pressure BEFORE overwriting lastWeather, so we can derive
      // a trend label (Rising/Falling/Steady) over the hourly refresh interval.
      const prevPressure = this.lastWeather && this.lastWeather.pressure_hpa;
      let pressureTrend = 'Steady';
      if (typeof prevPressure === 'number' && typeof c.surface_pressure === 'number') {
        const diff = c.surface_pressure - prevPressure;
        if (diff > 1.0)  pressureTrend = 'Rising';
        else if (diff < -1.0) pressureTrend = 'Falling';
      }
      this.lastWeather = {
        temp_f:         c.temperature_2m,
        feels_like_f:   c.apparent_temperature,
        humidity:       c.relative_humidity_2m,
        dewpoint_f:     c.dew_point_2m,
        weather_code:   c.weather_code,
        wind_mph:       c.wind_speed_10m,
        wind_deg:       c.wind_direction_10m,
        pressure_hpa:   c.surface_pressure,
        pressure_trend: pressureTrend,
        is_day:         c.is_day === 1,
        location:       WEATHER_LOCATION,
        fetched_at:     Date.now()
      };
      await this.state.storage.put('lastWeather', this.lastWeather);
    } catch (e) {
      console.error('weather fetch failed:', e && e.message);
    }
  }

  enrichStats(rawData) {
    // returns the stats object with outdoor weather injected if we have fresh-enough data
    if (!this.lastWeather) return rawData;
    if (Date.now() - this.lastWeather.fetched_at > WEATHER_STALE_MS) return rawData;
    const outdoor = {
      temp_f:         this.lastWeather.temp_f,
      feels_like_f:   this.lastWeather.feels_like_f,
      humidity:       this.lastWeather.humidity,
      dewpoint_f:     this.lastWeather.dewpoint_f,
      weather_code:   this.lastWeather.weather_code,
      wind_mph:       this.lastWeather.wind_mph,
      wind_deg:       this.lastWeather.wind_deg,
      pressure_hpa:   this.lastWeather.pressure_hpa,
      pressure_trend: this.lastWeather.pressure_trend,
      is_day:         this.lastWeather.is_day,
      location:       this.lastWeather.location,
      age_ms:         Date.now() - this.lastWeather.fetched_at
    };
    // Air quality piggybacks on the same outdoor block, gated independently
    // on its own freshness so a stale AQ fetch doesn't suppress weather and
    // vice versa. Same staleness window (2h) since both refresh hourly.
    if (this.lastAirQuality
        && Date.now() - this.lastAirQuality.fetched_at <= WEATHER_STALE_MS) {
      if (typeof this.lastAirQuality.us_aqi === 'number') {
        outdoor.us_aqi = this.lastAirQuality.us_aqi;
      }
      if (typeof this.lastAirQuality.pm2_5 === 'number') {
        outdoor.pm2_5 = this.lastAirQuality.pm2_5;
      }
      if (typeof this.lastAirQuality.co2_ppm === 'number') {
        outdoor.co2_ppm = this.lastAirQuality.co2_ppm;
      }
      if (typeof this.lastAirQuality.uv_index === 'number') {
        outdoor.uv_index = this.lastAirQuality.uv_index;
      }
    }
    return { ...rawData, outdoor };
  }

  badgeState() {
    // figure out what the badge should say + what color: live / stale / offline / maintenance
    const now = Date.now();
    if (now < this.maintenanceUntil) return { state: 'maintenance', color: '#c06b00' };
    if (!this.lastStats) return { state: 'offline', color: '#666' };
    if (now - this.lastStatsAt > 120000) return { state: 'stale', color: '#666' };
    return { state: 'live', color: '#2686e6' };
  }

  // ---- Snake leaderboard ----
  // Stored at DO storage key 'snake/leaderboard' as an array of up to 10
  // entries: { i: 'AAA' (3 chars), s: <score>, t: <unix-seconds> }.
  // Active games at 'snake/active:<gameId>' = { seed, t }, single-use,
  // 10-min TTL enforced on read. Replay verification keeps clients from
  // submitting scores they didn't earn: server replays the move log
  // against the seed and accepts only if the resulting score matches.

  _snakeRng(s) {
    s = (s + 0x6D2B79F5) | 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return [(((t ^ (t >>> 14)) >>> 0) / 4294967296), s];
  }

  _snakeSpawnFood(snake, state) {
    for (let i = 0; i < 200; i++) {
      const r1 = this._snakeRng(state); state = r1[1];
      const r2 = this._snakeRng(state); state = r2[1];
      const fx = Math.floor(r1[0] * 20);
      const fy = Math.floor(r2[0] * 20);
      if (!snake.some(p => p.x === fx && p.y === fy)) {
        return [{ x: fx, y: fy }, state];
      }
    }
    return [null, state];
  }

  _snakeReplay(seed, moves, claimedScore) {
    let state = seed | 0;
    let snake = [{ x: 10, y: 10 }, { x: 9, y: 10 }, { x: 8, y: 10 }];
    let dir = { x: 1, y: 0 };
    let score = 0;
    const sp = this._snakeSpawnFood(snake, state);
    let food = sp[0]; state = sp[1];
    if (!food) return false;

    // Move log entries: "<step><dir>" e.g. "12U", "47L".
    const parsed = [];
    for (const m of (Array.isArray(moves) ? moves : [])) {
      if (typeof m !== 'string') continue;
      const mm = /^(\d+)([UDLR])$/.exec(m);
      if (!mm) continue;
      const sn = parseInt(mm[1], 10);
      if (!Number.isInteger(sn) || sn < 0 || sn > SNAKE_MAX_STEPS) continue;
      parsed.push([sn, mm[2]]);
    }
    parsed.sort((a, b) => a[0] - b[0]);

    const D = { U: { x: 0, y: -1 }, D: { x: 0, y: 1 }, L: { x: -1, y: 0 }, R: { x: 1, y: 0 } };

    let mi = 0;
    for (let step = 0; step < SNAKE_MAX_STEPS; step++) {
      while (mi < parsed.length && parsed[mi][0] === step) {
        const nd = D[parsed[mi][1]];
        if (nd && (nd.x !== -dir.x || nd.y !== -dir.y)) dir = nd;
        mi++;
      }
      const head = { x: snake[0].x + dir.x, y: snake[0].y + dir.y };
      if (head.x < 0 || head.x >= 20 || head.y < 0 || head.y >= 20) {
        return score === claimedScore;
      }
      if (snake.some(p => p.x === head.x && p.y === head.y)) {
        return score === claimedScore;
      }
      snake.unshift(head);
      if (food && head.x === food.x && head.y === food.y) {
        score += 10;
        const sp2 = this._snakeSpawnFood(snake, state);
        food = sp2[0]; state = sp2[1];
        if (!food) return score === claimedScore; // board full
      } else {
        snake.pop();
      }
    }
    // ran past max-steps without dying → suspicious
    return false;
  }

  async _getSnakeLeaderboard() {
    const board = await this.state.storage.get('snake/leaderboard');
    return Array.isArray(board) ? board : [];
  }

  async _putSnakeLeaderboard(board) {
    await this.state.storage.put('snake/leaderboard', board);
  }

  // Today's-board: per-UTC-date list of all submissions that day. Used as a
  // "today's top scores" view alongside the all-time leaderboard. Storage
  // key kept as snake/daily-board:* for continuity with prior session data.
  _todayUtc() { return new Date().toISOString().slice(0, 10); }

  async _getDailyLeaderboard(date) {
    const board = await this.state.storage.get('snake/daily-board:' + date);
    return Array.isArray(board) ? board : [];
  }

  async _putDailyLeaderboard(date, board) {
    await this.state.storage.put('snake/daily-board:' + date, board);
  }

  // Seasonal rotation: at each calendar-quarter rollover, archive the current
  // all-time leaderboard under snake/season:Q<N>-YYYY and reset the active
  // board so a saturated leaderboard can't ossify forever. Past quarters are
  // browseable via /snake/seasons + /snake/season/leaderboard. UTC-anchored.
  _currentQuarter() {
    const d = new Date();
    return 'Q' + (Math.floor(d.getUTCMonth() / 3) + 1) + '-' + d.getUTCFullYear();
  }

  async _maybeRolloverSeason() {
    const current = this._currentQuarter();
    const lastArchived = await this.state.storage.get('snake/last-archived-quarter');
    if (!lastArchived) {
      // First run after deploy: stamp the current quarter so we don't archive
      // an empty board on the very first alarm tick.
      await this.state.storage.put('snake/last-archived-quarter', current);
      return;
    }
    if (lastArchived === current) return;
    // Quarter changed: snapshot the just-completed quarter's board (if it had
    // any scores) and reset the active leaderboard for the new quarter.
    const board = await this._getSnakeLeaderboard();
    if (board.length > 0) {
      await this.state.storage.put('snake/season:' + lastArchived, board);
    }
    await this._putSnakeLeaderboard([]);
    await this.state.storage.put('snake/last-archived-quarter', current);
  }

  // ASCII-art stats card served when curl/wget/httpie/libwww/PowerShell hit "/".
  // Built from the in-memory lastStats snapshot so there's zero ESP load on
  // each hit, and it still works when the device is down.
  buildCurlCard() {
    let s = null;
    try { if (this.lastStats) s = JSON.parse(this.lastStats); } catch (e) {}

    const num   = (v) => (Number.isFinite(v) ? Math.round(v).toLocaleString() : '-');
    const one   = (v, unit) => (Number.isFinite(v) ? v.toFixed(1) + (unit || '') : '-');
    const dash  = '-';

    const uptime   = s && s.uptime ? String(s.uptime) : dash;
    const tempF    = s && s.temperature && Number.isFinite(s.temperature.fahrenheit)
        ? one(s.temperature.fahrenheit, '°F') + ' · ' + one(s.temperature.celsius, '°C') : dash;
    const humidity = s && Number.isFinite(s.humidity_percent)
        ? one(s.humidity_percent, '%') : dash;
    const pressure = s && Number.isFinite(s.pressure_hpa)
        ? one(s.pressure_hpa, ' hPa') : dash;
    const co2      = s && Number.isFinite(s.co2_ppm)
        ? num(s.co2_ppm) + ' ppm (eCO₂)' : dash;
    const rssi     = s && Number.isFinite(s.rssi) ? s.rssi + ' dBm' : dash;
    const visitors = s ? num(s.visitors) + ' all-time · ' + num(s.daily_visitors) + ' today' : dash;
    const countries = s ? num(s.countries) : dash;
    const heapFree = s && s.memory && Number.isFinite(s.memory.used_percent)
        ? (100 - s.memory.used_percent).toFixed(0) + '% free' : dash;
    let sd = dash;
    if (s && Number.isFinite(s.sd_used_mb) && Number.isFinite(s.sd_free_mb)) {
        const totalMb = s.sd_used_mb + s.sd_free_mb;
        if (totalMb > 0) {
            sd = Math.round(s.sd_used_mb) + ' MB used / ' + Math.round(totalMb) + ' MB';
        }
    }

    const lines = [
      '',
      '   _    _      _ _       ______  _____ _____',
      '  | |  | |    | | |     |  ____|/ ____|  __ \\',
      '  | |__| | ___| | | ___ | |__  | (___ | |__) |',
      '  |  __  |/ _ \\ | |/ _ \\|  __|  \\___ \\|  ___/',
      '  | |  | |  __/ | | (_) | |____ ____) | |',
      '  |_|  |_|\\___|_|_|\\___/|______|_____/|_|',
      '',
      '  A website running on an ESP32 on a wall in Denver.',
      '',
      '  STATUS',
      '    Uptime       ' + uptime,
      '    Visitors     ' + visitors,
      '    Countries    ' + countries,
      '',
      '  INDOOR (sealed frame)',
      '    Temp         ' + tempF,
      '    Humidity     ' + humidity,
      '    Air          ' + co2,
      '    Pressure     ' + pressure,
    ];

    if (s && Number.isFinite(s.power_w)) {
      const fmtWh = (v) => {
        if (!Number.isFinite(v)) return dash;
        if (v >= 1000) return (v / 1000).toFixed(2) + ' kWh';
        return Math.round(v) + ' Wh';
      };
      lines.push(
        '',
        '  POWER (smart plug)',
        '    Now          ' + s.power_w.toFixed(1) + ' W',
        '    Today        ' + fmtWh(s.energy_today_wh),
        '    Lifetime     ' + fmtWh(s.energy_total_wh)
      );
    }

    if (s && s.outdoor && Number.isFinite(s.outdoor.temp_f)) {
      const o = s.outdoor;
      const loc = (o.location && typeof o.location === 'string') ? o.location : 'Denver, CO';
      lines.push(
        '',
        '  OUTDOOR (' + loc + ')',
        '    Temp         ' + one(o.temp_f, '°F'),
        '    Humidity     ' + (Number.isFinite(o.humidity) ? one(o.humidity, '%') : dash),
        '    Wind         ' + (Number.isFinite(o.wind_mph) ? one(o.wind_mph, ' mph') : dash)
      );
    }

    lines.push(
      '',
      '  DEVICE',
      '    Heap         ' + heapFree,
      '    SD card      ' + sd,
      '    WiFi         ' + rssi,
      '',
      '  LINKS',
      '    Web          https://helloesp.com',
      '    Guestbook    https://helloesp.com/guestbook',
      '    Console      https://helloesp.com/console',
      '    History      https://helloesp.com/history',
      '    Source       https://github.com/Tech1k/helloesp',
      '    RSS          https://helloesp.com/guestbook.rss',
      '    Badge        https://helloesp.com/status.svg',
      '',
      '  (You asked for it with curl. Nice.)',
      ''
    );
    return lines.join('\n');
  }

  buildStatusBadge(metric) {
    const s = this.badgeState();
    let stats = null;
    try { if (this.lastStats) stats = JSON.parse(this.lastStats); } catch (e) {}

    let valueText;
    if (s.state === 'maintenance') {
      valueText = 'maintenance';
    } else if (s.state === 'offline' || s.state === 'stale' || !stats) {
      valueText = 'offline';
    } else {
      switch (metric) {
        case 'visits':
          valueText = (stats.visitors != null ? stats.visitors : 0) + ' visits';
          break;
        case 'temp':
          if (stats.temperature && typeof stats.temperature.fahrenheit === 'number') {
            valueText = Math.round(stats.temperature.fahrenheit) + '\u00b0F';
          } else { valueText = 'no data'; }
          break;
        case 'power':
          // Live wattage from the Shelly poll (only present when shelly_url
          // is configured AND the freshness gate in buildStatsJson is met).
          // 'no data' covers both the forker case (no Shelly) and the stale
          // case (Shelly unreachable for >3 min) using the same fallback
          // the temp/visits cases use.
          if (typeof stats.power_w === 'number') {
            valueText = stats.power_w.toFixed(1) + ' W';
          } else { valueText = 'no data'; }
          break;
        case 'online':
          valueText = '\u25CF live';
          break;
        case 'uptime':
        default:
          valueText = formatUptime(stats.uptime || '');
          if (!valueText) valueText = 'up ?';
          break;
      }
    }

    // Approximate Verdana-11 character width ~6.5px. Label "HelloESP" fixed at 78px.
    const labelW = 78;
    const charW = 7;
    const valueW = Math.max(54, Math.round(valueText.length * charW + 20));
    const totalW = labelW + valueW;

    const safeValue = escapeHtml(valueText);
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${totalW}" height="20" viewBox="0 0 ${totalW} 20" role="img" aria-label="HelloESP: ${safeValue}"><title>HelloESP: ${safeValue}</title><linearGradient id="g" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient><clipPath id="r"><rect width="${totalW}" height="20" rx="3" fill="#fff"/></clipPath><g clip-path="url(#r)"><rect width="${labelW}" height="20" fill="#1a1a1a"/><rect x="${labelW}" width="${valueW}" height="20" fill="${s.color}"/><rect width="${totalW}" height="20" fill="url(#g)"/></g><g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11"><text x="${labelW/2}" y="14">HelloESP</text><text x="${labelW + valueW/2}" y="14">${safeValue}</text></g></svg>`;
  }

  buildStatusWide() {
    const s = this.badgeState();
    let stats = null;
    try { if (this.lastStats) stats = JSON.parse(this.lastStats); } catch (e) {}

    const W = 340, H = 78;
    const chipX = 14, chipY = 5;   // chip icon position
    const chipSize = 20;

    let line1Right;  // status indicator text + color
    if (s.state === 'maintenance')       line1Right = { text: 'maintenance', color: '#c06b00' };
    else if (s.state === 'live')         line1Right = { text: '\u25CF live', color: '#0a8b4a' };
    else                                 line1Right = { text: '\u25CF offline', color: '#b02030' };

    let row2 = '', row3 = '';
    if (stats && s.state !== 'offline' && s.state !== 'stale') {
      // Row A: indoor environment readings + power draw (when Shelly fresh)
      const tempF = stats.temperature && typeof stats.temperature.fahrenheit === 'number'
        ? Math.round(stats.temperature.fahrenheit) + '\u00b0F' : null;
      const hum = stats.humidity_percent != null ? Math.round(stats.humidity_percent) + '% RH' : null;
      const co2 = stats.co2_ppm != null ? stats.co2_ppm + ' CO\u2082 ppm' : null;
      const power = (typeof stats.power_w === 'number') ? Math.round(stats.power_w) + ' W' : null;
      const rowA = [tempF, hum, co2, power].filter(Boolean).join(' \u00b7 ');

      // Row B: ops/social (uptime, visits, countries, messages). Compact
      // uptime ("47d" / "8h" / "23m") and tight separators are needed to
      // fit all four within the 340px wide-card budget. Missing values
      // collapse the row gracefully via filter(Boolean).
      let up = null;
      const upMatch = String(stats.uptime || '').match(/(\d+)\s*days?[,\s]+(\d+)\s*hours?[,\s]+(\d+)\s*minutes?/);
      if (upMatch) {
        const d = +upMatch[1], h = +upMatch[2], mn = +upMatch[3];
        if (d > 0)       up = d + 'd';
        else if (h > 0)  up = h + 'h';
        else if (mn > 0) up = mn + 'm';
      }
      const vis = (typeof stats.visitors === 'number' && stats.visitors > 0)
        ? stats.visitors.toLocaleString() + ' visits' : null;
      const countries = (typeof stats.countries === 'number' && stats.countries > 0)
        ? stats.countries + ' countries' : null;
      const messages = (typeof stats.guestbook_approved === 'number' && stats.guestbook_approved > 0)
        ? stats.guestbook_approved.toLocaleString() + ' msgs' : null;
      const rowB = [up, vis, countries, messages].filter(Boolean).join(' \u00b7 ');

      row2 = escapeHtml(rowA);
      row3 = escapeHtml(rowB);
    } else {
      row2 = s.state === 'maintenance' ? 'Site under planned maintenance.' : 'Device is not currently reachable.';
      row3 = 'Check back in a moment.';
    }

    const statusColor = escapeHtml(line1Right.color);
    const statusText = escapeHtml(line1Right.text);

    return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="HelloESP status"><title>HelloESP status</title><clipPath id="rw"><rect width="${W}" height="${H}" rx="6" fill="#fff"/></clipPath><g clip-path="url(#rw)"><rect width="${W}" height="${H}" fill="#f8f7f4"/><rect width="${W}" height="28" fill="#1a1a1a"/></g><g transform="translate(${chipX} ${chipY}) scale(${chipSize/32})" fill="#2686e6">${CHIP_ICON_PATHS}</g><g font-family="Verdana,Geneva,DejaVu Sans,sans-serif" fill="#fff"><text x="42" y="19" font-size="12" font-weight="bold">HelloESP</text></g><g font-family="Verdana,Geneva,DejaVu Sans,sans-serif"><text x="${W - 14}" y="19" text-anchor="end" font-size="11" fill="${statusColor}">${statusText}</text><text x="14" y="50" font-size="12" fill="#1a1a1a">${row2}</text><text x="14" y="68" font-size="11" fill="#555">${row3}</text></g></svg>`;
  }

  broadcastEvent(eventName, jsonStr) {
    // Cap per-event payload size: with SSE_MAX_CLIENTS=500 viewers, a 1 MB
    // payload would be a 500 MB instantaneous fanout against a DO with a
    // ~128 MB memory ceiling. Drop oversized events instead of crashing the
    // DO; SSE delivery to all currently-connected clients is preserved for
    // every event that fits under the cap.
    const payload = new TextEncoder().encode(`event: ${eventName}\ndata: ${jsonStr}\n\n`);
    if (payload.byteLength > SSE_MAX_PAYLOAD) return;
    const dead = [];
    for (const w of this.sseClients) {
      w.write(payload).catch(() => dead.push(w));
    }
    for (const w of dead) {
      this.sseClients.delete(w);
      // Release the writer's transform-stream state. Without abort(), the
      // writable side stays held even after we forget the reference, which
      // leaks per-flapped-client over the DO's lifetime.
      w.abort().catch(() => {});
    }
  }

  async setMaintenance(minutes, message) {
    const m = Math.min(120, Math.max(0, Number(minutes) || 0));
    if (m === 0) {
      this.maintenanceUntil = 0;
      this.maintenanceMessage = '';
      await this.state.storage.delete('maintenanceUntil');
      await this.state.storage.delete('maintenanceMessage');
    } else {
      this.maintenanceUntil = Date.now() + m * 60000;
      this.maintenanceMessage = String(message || '').slice(0, 200);
      await this.state.storage.put('maintenanceUntil', this.maintenanceUntil);
      await this.state.storage.put('maintenanceMessage', this.maintenanceMessage);
    }
  }

  async verifyHmac(hexSig, nonce) {
    try {
      if (!hexSig || !/^[0-9a-f]{64}$/i.test(hexSig)) return false;
      const sig = new Uint8Array(32);
      for (let i = 0; i < 32; i++) sig[i] = parseInt(hexSig.slice(i * 2, i * 2 + 2), 16);
      const keyBytes = new TextEncoder().encode(this.env.HMAC_SECRET);
      const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
      return await crypto.subtle.verify('HMAC', key, sig, new TextEncoder().encode(nonce));
    } catch (e) {
      return false;
    }
  }

  async handleEvent(msg) {
    try {
      if (msg.event === 'maintenance') {
        await this.setMaintenance(msg.minutes, msg.message);
        return;
      }
      if (msg.event === 'stats_update') {
        if (msg.data) {
          const enriched = this.enrichStats(msg.data);
          this.lastStats = JSON.stringify(enriched);
          this.lastStatsAt = Date.now();
          // `clients` is per-broadcast (not cached in lastStats) so the
          // homepage presence indicator reflects current connections.
          const broadcastBody = JSON.stringify({ ...enriched, clients: this.sseClients.size });
          this.broadcastEvent('stats', broadcastBody);
        }
        return;
      }
      if (msg.event === 'console_update') {
        if (msg.data) this.broadcastEvent('console', JSON.stringify(msg.data));
        return;
      }
      if (msg.event && msg.event.startsWith('backup_')) {
        await this.handleBackupEvent(msg);
        return;
      }
      if (msg.event === 'r2_healthcheck') {
        await this._runR2Healthcheck();
        return;
      }
      if (msg.event === 'test_email') {
        await this._sendTestEmail();
        return;
      }
      if (msg.event === 'snake_clear') {
        // Admin reset of the snake leaderboards. Triggered from the device's
        // admin panel via the existing WS event channel; same trust model
        // as the other admin events above (must come over the HMAC-
        // authenticated WS). With the merged design (every play fills
        // today's-board, top scores cross-post to all-time), a useful clear
        // wipes both live boards plus orphaned replay storage in one shot.
        // Past daily-boards and season archives are intentionally preserved
        // so historical record stays intact.
        let ok = false;
        try {
          await this.state.storage.delete('snake/leaderboard');
          await this.state.storage.delete('snake/daily-board:' + this._todayUtc());
          // Sweep all stored replay moves (both boards just got wiped, so no
          // live references remain). list() returns up to 1000 keys per call;
          // typical move count is ~10-20 so a single page is enough.
          const moves = await this.state.storage.list({ prefix: 'snake/moves:' });
          const moveKeys = [];
          for (const key of moves.keys()) moveKeys.push(key);
          if (moveKeys.length) await this.state.storage.delete(moveKeys);
          ok = true;
        } catch (e) {
          console.error('snake_clear storage delete failed:', e && e.message);
        }
        if (this.espSocket && this.espSocket.readyState === 1) {
          try {
            this.espSocket.send(JSON.stringify({
              type: 'event', event: 'snake_clear_result', ok
            }));
          } catch (e) {}
        }
        return;
      }
      if (msg.event !== 'pending_guestbook') return;
      const env = this.env;
      if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;

      const now = Date.now();
      if (now - this.lastEmailAt < 300000) return;
      this.lastEmailAt = now;

      const count = Math.max(0, parseInt(msg.count, 10) || 0);
      if (count < 1) return;
      const noun = count === 1 ? 'entry' : 'entries';

      let body = `${count} guestbook ${noun} awaiting moderation on HelloESP.\n\n`;
      if (msg.name) {
        body += `Latest:\n`;
        body += `  ${String(msg.name).slice(0, 40)}`;
        if (msg.country && msg.country !== '??') body += ` (${String(msg.country).slice(0, 3)})`;
        body += `\n`;
        if (msg.message) body += `  "${String(msg.message).slice(0, 300)}"\n`;
        body += `\n`;
      }
      body += `To review, open /admin from your LAN.`;

      const res = await this._sendEmail({
        subject: `HelloESP: ${count} pending guestbook ${noun}`,
        text_body: body
      });
      if (res && !res.ok) console.error('SMTP2GO non-2xx:', res.status);
    } catch (e) {
      console.error('SMTP2GO send failed:', e && e.message);
    }
  }

  // --- Backup session accumulator (device streams chunked events; Worker reassembles, then
  // storeBackupBundle writes to R2 or falls back to emailBackupBundle if no R2 binding) ---

  pruneBackupSessions() {
    const cutoff = Date.now() - BACKUP_SESSION_IDLE;
    for (const [seq, s] of this.backupSessions) {
      if (s.startedAt < cutoff) this.backupSessions.delete(seq);
    }
  }

  async handleBackupEvent(msg) {
    const seq = msg.seq;
    if (typeof seq !== 'number') return;

    if (msg.event === 'backup_start') {
      this.pruneBackupSessions();
      this.backupSessions.set(seq, {
        startedAt: Date.now(),
        meta: {
          generated_at: String(msg.generated_at || ''),
          firmware: String(msg.firmware || ''),
          uptime: String(msg.uptime || '')
        },
        files: [],
        currentFile: null,
        totalB64: 0,
        chunkCount: 0,
        aborted: false
      });
      return;
    }

    const s = this.backupSessions.get(seq);
    if (!s || s.aborted) return;

    if (msg.event === 'backup_file_start') {
      s.currentFile = {
        name: String(msg.name || 'unknown'),
        size: Math.max(0, parseInt(msg.size, 10) || 0),
        chunks: []
      };
      return;
    }

    if (msg.event === 'backup_file_chunk') {
      if (!s.currentFile) return;
      const data = String(msg.data || '');
      s.currentFile.chunks.push(data);
      s.totalB64 += data.length;
      s.chunkCount++;
      if (s.totalB64 > BACKUP_MAX_B64 || s.chunkCount > BACKUP_MAX_CHUNKS) {
        s.aborted = true;
        this.backupSessions.delete(seq);
        console.error(`backup session ${seq} aborted: totalB64=${s.totalB64} chunks=${s.chunkCount}`);
      }
      return;
    }

    if (msg.event === 'backup_file_end') {
      if (!s.currentFile) return;
      s.files.push({
        name: s.currentFile.name,
        size: s.currentFile.size,
        content_b64: s.currentFile.chunks.join('')
      });
      s.currentFile = null;
      return;
    }

    if (msg.event === 'backup_file_skipped') {
      s.files.push({
        name: String(msg.name || 'unknown'),
        size: Math.max(0, parseInt(msg.size, 10) || 0),
        skipped: String(msg.reason || 'unknown')
      });
      return;
    }

    if (msg.event === 'backup_end') {
      const files = s.files;
      const meta = s.meta;
      const originalSize = Math.max(0, parseInt(msg.size, 10) || 0);
      this.backupSessions.delete(seq);
      await this.storeBackupBundle(meta, files, originalSize);
    }
  }

  // --- R2 write path ---
  //
  // Bundle layout on R2:
  //   state/YYYY-MM-DD/<file-path-from-device>
  //   state/YYYY-MM-DD/_manifest.json    (sha256 per file, firmware/uptime meta)
  //   state/latest.json                   (atomic pointer, written last = commit marker)
  //
  // Rotation (GFS): 7 daily + 4 weekly (Sun) + 12 monthly (1st) + yearly (Jan 1) forever.
  // Prefix + age guards refuse to delete anything recent or outside the state/YYYY-MM-DD/ namespace.

  static _b64ToBytes(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  static _hex(bytes) {
    let s = '';
    for (const b of bytes) s += b.toString(16).padStart(2, '0');
    return s;
  }

  _bucketDate(generated_at) {
    const m = /^(\d{4}-\d{2}-\d{2})/.exec(generated_at || '');
    return m ? m[1] : new Date().toISOString().slice(0, 10);
  }

  _shouldKeepSnapshot(dateStr, nowMs) {
    const d = new Date(dateStr + 'T00:00:00Z');
    if (isNaN(d.getTime())) return true; // malformed, err on keep
    const ageDays = Math.floor((nowMs - d.getTime()) / 86400000);
    if (ageDays < 8) return true;                                            // daily (last week)
    if (d.getUTCDay() === 0 && ageDays < 29) return true;                    // weekly (Sundays, 4wk)
    if (d.getUTCDate() === 1 && ageDays < 366) return true;                  // monthly (1st, 12mo)
    if (d.getUTCMonth() === 0 && d.getUTCDate() === 1) return true;          // yearly (Jan 1, forever)
    return false;
  }

  async storeBackupBundle(meta, files, originalSize) {
    const env = this.env;
    if (!env.BACKUP) {
      console.warn('R2 binding BACKUP not configured; falling back to email attachment path');
      return this.emailBackupBundle(meta, files, originalSize);
    }

    const date = this._bucketDate(meta.generated_at);
    const prefix = `state/${date}/`;

    const included = files.filter(f => !f.skipped && f.content_b64 !== undefined);
    const skipped = files.filter(f => f.skipped);

    const manifest = {
      schema: 'helloesp-backup/2',
      generated_at: meta.generated_at,
      firmware: meta.firmware,
      uptime: meta.uptime,
      date,
      original_size: originalSize,
      files: []
    };

    let bytesWritten = 0;
    try {
      // Filenames from the device must match a strict allowlist: alphanumeric,
      // dot/underscore/dash/slash only. This rejects path traversal (`..`),
      // leading separators, backslashes (Windows-style traversal), all control
      // chars including `\r\n` (which would corrupt manifest.json line keys),
      // and Unicode line separators (U+2028/U+2029). Any survivor is safe to
      // concat into both R2 keys and JSON manifest entries. The segment-must-
      // contain-an-alphanum check rejects degenerate names like `.` and `..`
      // which the regex alone would otherwise allow through.
      const SAFE_NAME_RE = /^[A-Za-z0-9._-]+(\/[A-Za-z0-9._-]+)*$/;
      const SEGMENT_HAS_ALNUM = /(^|\/)[A-Za-z0-9]/;
      for (const f of included) {
        if (typeof f.name !== 'string' || f.name.length === 0 || f.name.length > 256
            || f.name.startsWith('/') || f.name.includes('..')
            || !SAFE_NAME_RE.test(f.name) || !SEGMENT_HAS_ALNUM.test(f.name)) {
          console.warn(`backup ${date}: rejecting suspicious filename:`, JSON.stringify(f.name));
          manifest.files.push({ path: String(f.name).slice(0, 64), size: f.size, skipped: 'rejected_name' });
          continue;
        }
        const bytes = EspRelay._b64ToBytes(f.content_b64);
        const hashBuf = await crypto.subtle.digest('SHA-256', bytes);
        const sha256 = EspRelay._hex(new Uint8Array(hashBuf));
        await env.BACKUP.put(prefix + f.name, bytes);
        manifest.files.push({ path: f.name, size: bytes.length, sha256 });
        bytesWritten += bytes.length;
      }
      for (const f of skipped) {
        manifest.files.push({ path: f.name, size: f.size, skipped: f.skipped });
      }
      await env.BACKUP.put(prefix + '_manifest.json', JSON.stringify(manifest, null, 2), {
        httpMetadata: { contentType: 'application/json' }
      });
      // Atomic commit: latest.json update is the last write. If any earlier step failed, the
      // pointer still names whatever snapshot was last fully committed.
      const latest = {
        date,
        files: manifest.files.length,
        included: included.length,
        skipped: skipped.length,
        bytes: bytesWritten,
        at: Date.now(),
        firmware: meta.firmware,
        generated_at: meta.generated_at
      };
      await env.BACKUP.put('state/latest.json', JSON.stringify(latest, null, 2), {
        httpMetadata: { contentType: 'application/json' }
      });
    } catch (e) {
      const reason = (e && e.message) || String(e);
      console.error(`backup ${date} R2 write failed:`, reason);
      await this._sendBackupFailureAlert(date, reason);
      return;
    }

    this.lastBackupAt = Date.now();
    this.lastBackupDate = date;
    await this.state.storage.put('lastBackupAt', this.lastBackupAt);
    await this.state.storage.put('lastBackupDate', date);

    // Tell the device the bundle was actually stored (not just sent).
    if (this.espSocket && this.espSocket.readyState === 1) {
      try {
        this.espSocket.send(JSON.stringify({
          type: 'event',
          event: 'backup_committed',
          date,
          bytes: bytesWritten,
          files: manifest.files.length,
          included: included.length,
          skipped: skipped.length,
          at: this.lastBackupAt
        }));
      } catch (e) {
        console.error('backup_committed push failed:', e && e.message);
      }
    }

    // Fire-and-forget rotation. Its failure is logged but doesn't invalidate the committed backup.
    this._rotateSnapshots().catch(e => console.error('rotation failed:', e && e.message));
  }

  async _rotateSnapshots() {
    const env = this.env;
    if (!env.BACKUP) return;
    const now = Date.now();

    // Discover dated snapshot folders via list+delimiter.
    const listing = await env.BACKUP.list({ prefix: 'state/', delimiter: '/' });
    const folders = [];
    for (const p of (listing.delimitedPrefixes || [])) {
      const m = /^state\/(\d{4}-\d{2}-\d{2})\/$/.exec(p);
      if (m) folders.push(m[1]);
    }

    const toDelete = folders.filter(d => {
      if (this._shouldKeepSnapshot(d, now)) return false;
      const ageDays = Math.floor((now - new Date(d + 'T00:00:00Z').getTime()) / 86400000);
      return ageDays >= 8; // hard floor; never prune recent even if classifier says drop
    });

    for (const date of toDelete) {
      // Belt-and-suspenders: iterate each object under the date prefix and verify the key
      // shape before deleting. Refuse anything outside state/YYYY-MM-DD/.
      let cursor;
      do {
        const sub = await env.BACKUP.list({ prefix: `state/${date}/`, cursor });
        const keys = (sub.objects || [])
          .map(o => o.key)
          .filter(k => /^state\/\d{4}-\d{2}-\d{2}\//.test(k));
        if (keys.length) await env.BACKUP.delete(keys);
        cursor = sub.truncated ? sub.cursor : undefined;
      } while (cursor);
    }
  }

  async _sendBackupFailureAlert(date, reason) {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    const now = Date.now();
    if (now - this.lastBackupFailureEmailAt < 3600000) return; // one per hour at most
    this.lastBackupFailureEmailAt = now;
    try {
      await this._sendEmail({
        subject: `HelloESP backup FAILED - ${date}`,
        text_body: `Backup for ${date} could not be written to R2.\n\nReason: ${reason}\n\nCheck the R2 bucket and Worker logs.`
      });
    } catch (e) {
      console.error('backup failure email send failed:', e && e.message);
    }
  }

  // Admin-triggered SMTP2GO test. Sends a harmless test email; reports back whether SMTP2GO
  // accepted it. Catches silent failures (wrong key, unverified sender, etc.) before a real
  // alert needs to fire.
  async _sendTestEmail() {
    const env = this.env;
    const sendResult = (pass, detail) => {
      if (this.espSocket && this.espSocket.readyState === 1) {
        try {
          // ESP uses naive indexOf-based JSON field extraction, so strip any characters it
          // can't round-trip (embedded quotes / backslashes / control chars would terminate early).
          const safe = String(detail).replace(/["\\\n\r\t\x00-\x1f]/g, ' ').slice(0, 128);
          this.espSocket.send(JSON.stringify({
            type: 'event',
            event: 'test_email_result',
            pass,
            detail: safe
          }));
        } catch (e) {
          console.error('test_email_result push failed:', e && e.message);
        }
      }
    };

    if (!env.SMTP2GO_KEY) { sendResult(false, 'SMTP2GO_KEY not set'); return; }
    if (!env.NOTIFY_EMAIL) { sendResult(false, 'NOTIFY_EMAIL not set'); return; }

    try {
      const res = await this._sendEmail({
        subject: 'HelloESP test email',
        text_body: `This is a manual test sent from the admin panel at ${new Date().toISOString()}.\n\nIf you received this, SMTP2GO is working. Dead-man, backup, and guestbook alerts will use the same path.`
      });
      if (!res || !res.ok) {
        let bodyText = '';
        try { if (res) bodyText = (await res.text()).slice(0, 100); } catch (e) {}
        sendResult(false, `SMTP2GO HTTP ${res ? res.status : 'no-response'}${bodyText ? ': ' + bodyText : ''}`);
        return;
      }
      sendResult(true, `sent to ${env.NOTIFY_EMAIL}`);
    } catch (e) {
      sendResult(false, 'fetch failed: ' + ((e && e.message) || String(e)));
    }
  }

  // Admin-triggered R2 liveness test. PUTs a small file, reads it back byte-for-byte, deletes.
  // Sends the outcome back to the ESP as an event so the admin UI can display it.
  // The test key lives outside the state/YYYY-MM-DD/ rotation namespace, so rotation won't touch it.
  async _runR2Healthcheck() {
    const env = this.env;
    const sendResult = (pass, detail) => {
      if (this.espSocket && this.espSocket.readyState === 1) {
        try {
          const safe = String(detail).replace(/["\\\n\r\t\x00-\x1f]/g, ' ').slice(0, 128);
          this.espSocket.send(JSON.stringify({
            type: 'event',
            event: 'r2_healthcheck_result',
            pass,
            detail: safe
          }));
        } catch (e) {
          console.error('r2_healthcheck_result push failed:', e && e.message);
        }
      }
    };

    if (!env.BACKUP) {
      sendResult(false, 'R2 binding not configured');
      return;
    }

    const key = 'state/_healthcheck/test.txt';
    const payload = `helloesp r2 healthcheck ${Date.now()}`;

    try {
      await env.BACKUP.put(key, payload);
    } catch (e) {
      sendResult(false, 'PUT failed: ' + ((e && e.message) || String(e)));
      return;
    }
    try {
      const obj = await env.BACKUP.get(key);
      if (!obj) { sendResult(false, 'GET returned null'); return; }
      const text = await obj.text();
      if (text !== payload) { sendResult(false, `readback mismatch (${text.length} vs ${payload.length} bytes)`); return; }
    } catch (e) {
      sendResult(false, 'GET failed: ' + ((e && e.message) || String(e)));
      return;
    }
    try {
      await env.BACKUP.delete(key);
    } catch (e) {
      // non-fatal: put/get confirmed the bucket works. Leftover test file gets overwritten next run.
      console.warn('r2 healthcheck delete failed:', e && e.message);
    }
    sendResult(true, 'put/get/delete ok');
  }

  async _maybeSendMissedBackupAlert() {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    // Use lastBackupAt if any successful backup has happened; otherwise use
    // the DO's first-seen time as the reference. Without this, a fresh
    // deploy that never gets a successful backup would never alert.
    const referenceTime = this.lastBackupAt || this.firstSeenAt;
    if (!referenceTime) return;
    const now = Date.now();
    const ageMs = now - referenceTime;
    if (ageMs < 48 * 3600000) return;                              // fresh
    if (now - this.lastBackupMissedEmailAt < 24 * 3600000) return; // one per day
    this.lastBackupMissedEmailAt = now;
    const ageHours = Math.floor(ageMs / 3600000);
    const neverHadOne = !this.lastBackupAt;
    try {
      await this._sendEmail({
        subject: neverHadOne
          ? `HelloESP backup never succeeded (${ageHours}h since deploy)`
          : `HelloESP backup overdue (${ageHours}h)`,
        text_body: neverHadOne
          ? `Worker has been running for ${ageHours} hours and no R2 backup has ever succeeded.\n\nCheck that the R2 binding is configured (wrangler.toml) and the device is uploading bundles.`
          : `No successful backup since ${new Date(this.lastBackupAt).toISOString()}.\n\nLast snapshot date: ${this.lastBackupDate || 'unknown'}.\n\nCheck that the device is online and the R2 binding is healthy.`
      });
    } catch (e) {
      console.error('missed-backup email send failed:', e && e.message);
    }
  }

  async emailBackupBundle(meta, files, originalSize) {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) {
      console.warn('backup bundle received but SMTP2GO_KEY or NOTIFY_EMAIL unset');
      return;
    }

    const bundle = {
      schema: 'helloesp-backup/1',
      generated_at: meta.generated_at,
      firmware: meta.firmware,
      uptime: meta.uptime,
      original_size: originalSize,
      files
    };
    const bundleJson = JSON.stringify(bundle);
    // BACKUP_PART_SIZE is a byte budget (SMTP attachment ceiling). Slicing
    // bundleJson.slice() slices by UTF-16 code units, so any non-ASCII content
    // (e.g. a guestbook entry with an em-dash) would make a part over-size or
    // split a code point. Encode once and slice the resulting byte buffer.
    const bundleBytes = new TextEncoder().encode(bundleJson);
    const date = (meta.generated_at || new Date().toISOString()).slice(0, 10);
    // 6-char unique session token for grouping multipart email backups.
    // Not security-critical (just a uniqueness key) but switched to
    // crypto-RNG to satisfy CodeQL's insecure-randomness check across the codebase.
    const sessionId = crypto.randomUUID().replace(/-/g, '').slice(0, 6);
    const totalParts = Math.max(1, Math.ceil(bundleBytes.length / BACKUP_PART_SIZE));
    const padWidth = String(totalParts).length;

    const skipped = files.filter(f => f.skipped);
    const included = files.filter(f => !f.skipped);

    let header = `HelloESP weekly state backup.\n\n`;
    header += `Generated:   ${meta.generated_at || 'n/a'}\n`;
    header += `Firmware:    ${meta.firmware || 'n/a'}\n`;
    header += `Uptime:      ${meta.uptime || 'n/a'}\n`;
    header += `Files:       ${included.length} included, ${skipped.length} skipped\n`;
    header += `Bundle size: ${(bundleBytes.length / 1024).toFixed(1)} KB total\n`;
    header += `Raw size:    ${(originalSize / 1024).toFixed(1)} KB (on device)\n`;
    if (totalParts > 1) header += `Parts:       ${totalParts} emails (session ${sessionId})\n`;
    header += `\n`;
    if (skipped.length) {
      header += `Skipped:\n`;
      for (const f of skipped) header += `  - ${f.name} (${f.size} bytes, ${f.skipped})\n`;
      header += `\n`;
    }

    let footer;
    if (totalParts === 1) {
      footer = `Contents are base64-encoded inside the JSON. To restore a specific file:\n`;
      footer += `  jq -r '.files[] | select(.name=="guestbook.csv") | .content_b64' backup.json | base64 -d > guestbook.csv\n`;
    } else {
      footer = `This bundle is split across ${totalParts} emails. Download every attachment,\n`;
      footer += `then reassemble and restore:\n`;
      footer += `  cat helloesp-backup-${date}-${sessionId}-part*.json > bundle.json\n`;
      footer += `  jq -r '.files[] | select(.name=="guestbook.csv") | .content_b64' bundle.json | base64 -d > guestbook.csv\n`;
      footer += `\nIf a part is missing, the device retries the full backup next week.\n`;
    }

    let sentParts = 0;
    for (let i = 0; i < totalParts; i++) {
      const bytes = bundleBytes.subarray(i * BACKUP_PART_SIZE, (i + 1) * BACKUP_PART_SIZE);
      let bin = '';
      const CHUNK = 0x8000;
      for (let j = 0; j < bytes.length; j += CHUNK) {
        bin += String.fromCharCode.apply(null, bytes.subarray(j, j + CHUNK));
      }
      const attachmentB64 = btoa(bin);

      const partNum = String(i + 1).padStart(padWidth, '0');
      const totalStr = String(totalParts).padStart(padWidth, '0');
      const filename = totalParts === 1
        ? `helloesp-backup-${date}.json`
        : `helloesp-backup-${date}-${sessionId}-part${partNum}of${totalStr}.json`;
      const subject = totalParts === 1
        ? `HelloESP backup - ${date} (${(bundleBytes.length / 1024).toFixed(0)} KB)`
        : `HelloESP backup - ${date} (part ${i + 1}/${totalParts})`;

      let body = header;
      if (totalParts > 1) {
        body += `>>> This is part ${i + 1} of ${totalParts}. Slice offset: ${i * BACKUP_PART_SIZE} of ${bundleBytes.length} bytes.\n\n`;
      }
      body += footer;

      try {
        const res = await this._sendEmail({
          subject,
          text_body: body,
          attachments: [{
            filename,
            fileblob: attachmentB64,
            mimetype: 'application/json'
          }]
        });
        if (!res || !res.ok) {
          console.error(`backup part ${i + 1}/${totalParts} SMTP2GO non-2xx:`, res ? res.status : 'no-response');
          continue;
        }
        sentParts++;
      } catch (e) {
        console.error(`backup part ${i + 1}/${totalParts} failed:`, e && e.message);
      }

      if (i < totalParts - 1) await new Promise(r => setTimeout(r, BACKUP_PART_DELAY_MS));
    }

    if (sentParts === totalParts) {
      this.lastBackupAt = Date.now();
      this.lastBackupDate = date;
      await this.state.storage.put('lastBackupAt', this.lastBackupAt);
      await this.state.storage.put('lastBackupDate', date);
    } else {
      console.error(`backup partial: ${sentParts}/${totalParts} parts emailed`);
    }
  }

  async alarm() {
    // Seasonal rollover check (cheap: 1-2 storage reads, no-op when in-quarter)
    this._maybeRolloverSeason().catch(() => {});

    // lazy weather refresh: fetch on first tick, then every WEATHER_REFRESH_MS (1 hour)
    if (!this.lastWeather || Date.now() - this.lastWeather.fetched_at > WEATHER_REFRESH_MS) {
      this.refreshWeather().catch(() => {});
    }
    // Same cadence for air quality (Open-Meteo updates both hourly).
    if (!this.lastAirQuality || Date.now() - this.lastAirQuality.fetched_at > WEATHER_REFRESH_MS) {
      this.refreshAirQuality().catch(() => {});
    }

    // dead-man's-switch: email if ESP has been silent for >24h
    this.maybeSendDeadmanAlert().catch(() => {});

    // overdue-backup alert: email if last successful backup is >48h old (once per day)
    this._maybeSendMissedBackupAlert().catch(() => {});

    // dead-client sweep: if ESP isn't pushing events, broadcasts don't prune dead SSE writers.
    // Send a zero-cost SSE comment to every client; prune any that throw.
    if (this.sseClients.size > 0) {
      const ping = new TextEncoder().encode(': ping\n\n');
      const dead = [];
      for (const w of this.sseClients) {
        w.write(ping).catch(() => dead.push(w));
      }
      for (const w of dead) {
        this.sseClients.delete(w);
        w.abort().catch(() => {});
      }
    }

    if (this.espSocket && Date.now() - this.lastActivity > 75000) {
      try { this.espSocket.close(); } catch (e) {}
      this.espSocket = null;
    }

    // Sweep orphan backup sessions whose device dropped mid-stream. Without
    // this, a half-finished session sits in memory until the next backup_start
    // (up to 24h on the daily cadence). Bounded but wasteful.
    this.pruneBackupSessions();

    // Time-based eviction of expired rate-limit and ws-auth-fail entries.
    // The opportunistic size-gated sweep inside _enforceRateLimit only fires
    // past 500 entries; on long stretches of low traffic, expired buckets
    // would otherwise linger indefinitely below that threshold.
    {
      const t = Date.now();
      for (const [k, v] of this.rateLimits) {
        if (v.resetAt < t) this.rateLimits.delete(k);
      }
      for (const [k, v] of this.wsAuthFails) {
        if (v.blockedUntil < t && t - v.firstAt > 600000) this.wsAuthFails.delete(k);
      }
    }

    // Persist lastActivity so isolate eviction doesn't blank the deadman state.
    // Throttled to ~30s (alarm cadence). One write per tick; storage cost is
    // negligible vs the cost of a missed deadman alert.
    if (this.lastActivity > 0) {
      this.state.storage.put('lastActivity', this.lastActivity).catch(() => {});
    }

    // Sweep stale snake game seeds (>10 min old, never submitted). Keeps
    // storage tidy without affecting the leaderboard, which lives at a
    // different key prefix and is permanent. Cheap: list+filter+delete a
    // handful of small keys, runs every 30s alongside the SSE ping.
    try {
      const stale = await this.state.storage.list({ prefix: 'snake/active:' });
      const cutoff = Date.now() - 10 * 60 * 1000;
      const toDelete = [];
      for (const [key, val] of stale) {
        if (val && typeof val.t === 'number' && val.t < cutoff) toDelete.push(key);
      }
      if (toDelete.length) await this.state.storage.delete(toDelete);
    } catch (e) {}

    // Always rearm. Without this, an offline-ESP + no-SSE-clients state would stop the alarm
    // loop, and dead-man / overdue-backup alerts would be delayed until the next visitor woke
    // the DO. ~86k invocations/month is well under Worker free-tier limits. Direct setAlarm
    // here (not _ensureAlarm). This IS the rearm: we want a fresh 30s window, not to defer
    // to a farther-out existing alarm.
    await this.state.storage.setAlarm(Date.now() + 30000);
  }

  async fetch(request) {
    const url = new URL(request.url);

    // curl/wget/httpie/PowerShell hitting "/" get a text/plain ASCII stats
    // card built from the cached lastStats. Zero ESP load, works when the
    // device is down, no-store so it doesn't poison "/" for browser visits.
    if (request.method === 'GET' && (url.pathname === '/' || url.pathname === '')) {
      const ua = (request.headers.get('User-Agent') || '').toLowerCase();
      if (/\b(curl|wget|httpie|libwww-perl|powershell)\b/.test(ua)) {
        return new Response(this.buildCurlCard(), {
          status: 200,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-store',
            ...SEC_HEADERS
          }
        });
      }
    }

    // Worker-side load-shedding endpoints.
    if (url.pathname === '/ping' && request.method === 'GET') {
      const espUp = !!(this.espSocket && this.espSocket.readyState === 1);
      return new Response(espUp ? 'pong' : 'offline', {
        status: espUp ? 200 : 503,
        headers: {
          'Content-Type': 'text/plain',
          'Cache-Control': 'no-store',
          ...SEC_HEADERS
        }
      });
    }

    // /stats: serve from Worker's cached lastStats (pushed by ESP via SSE events).
    if (url.pathname === '/stats' && request.method === 'GET') {
      if (this.lastStats) {
        const age = Date.now() - this.lastStatsAt;
        // Inject fresh sseClients.size for the live-presence indicator.
        // Stays out of lastStats itself so cache hits / cold-relay paths
        // also pick up the current count rather than a stale snapshot.
        let body = this.lastStats;
        try {
          const parsed = JSON.parse(this.lastStats);
          parsed.clients = this.sseClients.size;
          body = JSON.stringify(parsed);
        } catch (e) { /* fall through to raw */ }
        return new Response(body, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=5, stale-while-revalidate=30',
            'X-Worker-Cache-Age': String(Math.floor(age / 1000)),
            ...SEC_HEADERS
          }
        });
      }
      // No cached stats yet; fall through to the ESP relay below.
    }

    // Embeddable live status badges. Uses cached lastStats; zero ESP load.
    if (url.pathname === '/status.svg') {
      const metric = url.searchParams.get('metric') || 'uptime';
      const svg = this.buildStatusBadge(metric);
      return new Response(svg, {
        status: 200,
        headers: {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          'Cache-Control': 'public, max-age=60',
          ...SEC_HEADERS
        }
      });
    }

    // ---- Snake leaderboard endpoints ----
    // CORS: read endpoints allow cross-origin GETs (so the snake game on
    // 404.html served direct via LAN can still fetch the leaderboard from
    // helloesp.com), submission is same-origin enforced by replay verification
    // not by Origin header (a forged Origin doesn't help if the moves don't
    // replay to the claimed score).
    if (url.pathname === '/snake/leaderboard' && request.method === 'GET') {
      const board = await this._getSnakeLeaderboard();
      return new Response(JSON.stringify(board), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=30',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/snake/seed' && request.method === 'GET') {
      const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const limited = this._enforceRateLimit(clientIP);
      if (limited) return limited;
      // Cryptographically secure RNG for the seed and gameId.
      const seedArr = new Uint32Array(1);
      crypto.getRandomValues(seedArr);
      const seed = seedArr[0] | 0;
      const gameId = Date.now().toString(36) + '-' + crypto.randomUUID().replace(/-/g, '').slice(0, 12);
      // Persist only the seed and timestamp; no IP. The active entry is just
      // a single-use replay credential paired with /snake/score, so the IP
      // would be dead data and an unnecessary durable storage of a PII-ish
      // identifier. Rate-limit/abuse handling is in-memory upstream of this.
      await this.state.storage.put('snake/active:' + gameId, {
        seed, t: Date.now()
      });
      return new Response(JSON.stringify({ seed, gameId }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/snake/daily/leaderboard' && request.method === 'GET') {
      const dateParam = url.searchParams.get('date');
      const date = (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam))
        ? dateParam : this._todayUtc();
      const board = await this._getDailyLeaderboard(date);
      return new Response(JSON.stringify({ date, leaderboard: board }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=30',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    // List archived seasons (past quarters with leaderboards). Sorted
    // descending (most recent quarter first). Includes the current quarter
    // identifier so the client can label which quarter is currently active.
    if (url.pathname === '/snake/seasons' && request.method === 'GET') {
      const list = await this.state.storage.list({ prefix: 'snake/season:' });
      const quarters = [];
      for (const key of list.keys()) {
        quarters.push(key.replace('snake/season:', ''));
      }
      quarters.sort((a, b) => {
        const am = a.match(/^Q(\d)-(\d+)$/);
        const bm = b.match(/^Q(\d)-(\d+)$/);
        if (!am || !bm) return 0;
        return (parseInt(bm[2], 10) - parseInt(am[2], 10))
            || (parseInt(bm[1], 10) - parseInt(am[1], 10));
      });
      return new Response(JSON.stringify({ quarters, current: this._currentQuarter() }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=300',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/snake/season/leaderboard' && request.method === 'GET') {
      const quarter = url.searchParams.get('quarter');
      if (!quarter || !/^Q[1-4]-\d{4}$/.test(quarter)) {
        return new Response(JSON.stringify({ error: 'bad quarter' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...SEC_HEADERS }
        });
      }
      const board = await this.state.storage.get('snake/season:' + quarter);
      return new Response(JSON.stringify({ quarter, leaderboard: Array.isArray(board) ? board : [] }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=3600',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    // Replay viewer: returns the stored seed + moves for a given gameId so
    // the client can deterministically replay a top-scoring game. Only
    // games still on a top-10 board have moves stored; dropped entries are
    // cleaned up automatically on each new score that displaces them.
    if (url.pathname === '/snake/replay' && request.method === 'GET') {
      const gameId = url.searchParams.get('gameId');
      if (!gameId || !/^[A-Za-z0-9-]{1,64}$/.test(gameId)) {
        return new Response(JSON.stringify({ error: 'bad gameId' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...SEC_HEADERS }
        });
      }
      const replay = await this.state.storage.get('snake/moves:' + gameId);
      if (!replay) {
        return new Response(JSON.stringify({ error: 'not found' }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...SEC_HEADERS }
        });
      }
      return new Response(JSON.stringify({ seed: replay.seed, moves: replay.moves }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          // Replays are immutable; long cache OK.
          'Cache-Control': 'public, max-age=86400',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/snake/score' && request.method === 'POST') {
      const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const limited = this._enforceRateLimit(clientIP);
      if (limited) return limited;
      const corsHdrs = {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        'Access-Control-Allow-Origin': '*',
        ...SEC_HEADERS
      };
      const fail = (msg, status) => new Response(JSON.stringify({ ok: false, error: msg }), {
        status: status || 400, headers: corsHdrs
      });

      let body;
      try { body = await request.json(); } catch (e) { return fail('bad json'); }
      const { gameId, initials, score, moves } = body || {};
      if (typeof gameId !== 'string' || gameId.length === 0 || gameId.length > 64) return fail('bad gameId');
      if (typeof initials !== 'string') return fail('bad initials');
      if (typeof score !== 'number' || !Number.isFinite(score) || score < 10 || score > 100000) return fail('bad score');
      if (!Array.isArray(moves) || moves.length > SNAKE_MAX_STEPS) return fail('bad moves');

      // Strict-validate initials. Earlier code silently truncated ("AAAB" -> "AAA")
      // which let bypass-via-fetch submit different initials than the user typed.
      // The client form's maxlength=3 + filter already pre-cleans, so a strict
      // worker check just rejects bypass attempts and surfaces them as a form
      // error rather than corrupting the leaderboard.
      const upper = initials.toUpperCase();
      if (!/^[A-Z0-9]{3}$/.test(upper)) return fail('initials must be exactly 3 alphanumeric chars');
      const cleanInitials = upper;
      if (BLOCKED_INITIALS.has(cleanInitials)) return fail('try different initials');

      // Serialize the critical section (active-game check, single-use delete,
      // replay verify, leaderboard read-modify-write). Without this:
      //  (a) Two parallel POSTs with the same gameId both pass the active check
      //      and double-write to the leaderboard.
      //  (b) Two simultaneous valid submissions race the board RMW and the
      //      last-writer-wins drops one entry.
      let respPayload;
      let respStatus = 200;
      await this.state.blockConcurrencyWhile(async () => {
        const active = await this.state.storage.get('snake/active:' + gameId);
        if (!active) { respPayload = { ok: false, error: 'game not found or expired' }; respStatus = 400; return; }
        if (Date.now() - active.t > 10 * 60 * 1000) {
          await this.state.storage.delete('snake/active:' + gameId);
          respPayload = { ok: false, error: 'game expired' }; respStatus = 400; return;
        }
        // Replay verify: server applies moves to the seeded game, accepts
        // only if the resulting score matches the claim. This is what stops
        // trivial "POST /snake/score {score: 9999}" cheats.
        const ok = this._snakeReplay(active.seed, moves, score);
        // Single-use: delete the active game whether replay passed or not.
        await this.state.storage.delete('snake/active:' + gameId);
        if (!ok) { respPayload = { ok: false, error: 'score does not match game' }; respStatus = 400; return; }

        // Every score lands on today's-board (filling the daily activity view)
        // AND cross-posts to all-time if it qualifies for the top 10. Same gameId
        // on both boards, so the replay viewer works from either context.
        const todayDate = this._todayUtc();
        const entry = { i: cleanInitials, s: score, t: Math.floor(Date.now() / 1000), g: gameId, d: todayDate };
        const sortFn = (a, b) => (b.s - a.s) || (a.t - b.t);

        const oldToday = await this._getDailyLeaderboard(todayDate);
        const oldAllTime = await this._getSnakeLeaderboard();

        let todayBoard = oldToday.concat([entry]);
        todayBoard.sort(sortFn);
        todayBoard = todayBoard.slice(0, 10);
        await this._putDailyLeaderboard(todayDate, todayBoard);
        const todayIdx = todayBoard.findIndex(e => e === entry);
        const todayRank = todayIdx >= 0 ? todayIdx + 1 : null;

        let alltimeRank = null;
        let finalAllTime = oldAllTime;
        if (oldAllTime.length < 10 || score > oldAllTime[oldAllTime.length - 1].s) {
          finalAllTime = oldAllTime.concat([entry]);
          finalAllTime.sort(sortFn);
          finalAllTime = finalAllTime.slice(0, 10);
          await this._putSnakeLeaderboard(finalAllTime);
          const idx = finalAllTime.findIndex(e => e === entry);
          alltimeRank = idx >= 0 ? idx + 1 : null;
        }

        const onToday = todayBoard.some(e => e.g === gameId);
        const onAllTime = finalAllTime.some(e => e.g === gameId);
        if (onToday || onAllTime) {
          await this.state.storage.put('snake/moves:' + gameId, { seed: active.seed, moves });
        }

        // Cleanup: delete moves only for entries gone from BOTH today's-board
        // and all-time AND not pinned by their tagged origin daily-board (a
        // cross-posted entry from a prior day stays on its origin board even
        // after displacement from all-time).
        const liveIds = new Set();
        for (const e of todayBoard) if (e.g) liveIds.add(e.g);
        for (const e of finalAllTime) if (e.g) liveIds.add(e.g);
        const seen = new Set();
        for (const e of [...oldToday, ...oldAllTime]) {
          if (!e.g || seen.has(e.g)) continue;
          seen.add(e.g);
          if (liveIds.has(e.g)) continue;
          if (e.d && e.d !== todayDate) {
            const otherBoard = await this._getDailyLeaderboard(e.d);
            if (otherBoard.some(x => x.g === e.g)) continue;
          }
          await this.state.storage.delete('snake/moves:' + e.g);
        }

        respPayload = {
          ok: true,
          today: { board: todayBoard, rank: todayRank, date: todayDate },
          alltime: { board: finalAllTime, rank: alltimeRank }
        };
      });
      return new Response(JSON.stringify(respPayload), { status: respStatus, headers: corsHdrs });
    }

    // CORS preflight for /snake/score (POST + JSON triggers preflight when cross-origin).
    if (url.pathname === '/snake/score' && request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Max-Age': '600'
        }
      });
    }

    // Guestbook inline translation. Powered by Workers AI @cf/meta/m2m100-1.2b
    // when env.AI is bound (see wrangler.toml [ai] block). Cached per
    // (id, target) in DO storage so each unique pair costs at most one neuron.
    // CORS-permissive so the LAN-served guestbook page can hit the Worker
    // cross-origin (visitors loaded via direct LAN IP).
    if (url.pathname === '/guestbook/translate') {
      const corsHdrs = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '600'
      };
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: corsHdrs });
      }
      if (request.method !== 'POST') {
        return new Response('Method not allowed', { status: 405, headers: corsHdrs });
      }
      if (!this.env.AI) {
        return new Response('Translation unavailable (AI binding not configured)',
          { status: 503, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }

      // Per-IP rate limit so a single client can't drain the daily neuron
      // budget. Reuses the existing rateLimits map. Translation is heavier
      // than a normal relay, so use a tighter cap (10 / minute / IP).
      const tIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const tNow = Date.now();
      let trl = this.rateLimits.get('xlate:' + tIP);
      if (!trl || tNow > trl.resetAt) {
        trl = { count: 0, resetAt: tNow + 60000 };
        this.rateLimits.set('xlate:' + tIP, trl);
      }
      trl.count++;
      if (trl.count > 10) {
        return new Response('Translation rate limit exceeded; try again in a minute',
          { status: 429, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }

      let body;
      try { body = await request.json(); } catch (e) {
        return new Response('Invalid JSON', { status: 400, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }
      const id = body && typeof body.id === 'string' ? body.id.slice(0, 32) : '';
      const text = body && typeof body.text === 'string' ? body.text : '';
      const target = body && typeof body.target === 'string' ? body.target.toLowerCase().slice(0, 5) : '';
      const source = body && typeof body.source === 'string' ? body.source.toLowerCase().slice(0, 5) : '';
      if (!text || text.length > 1000) {
        return new Response('Text empty or too long (max 1000 chars)',
          { status: 400, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }
      if (!/^[a-z]{2,3}(-[a-z]{2,3})?$/.test(target)) {
        return new Response('Invalid target language code',
          { status: 400, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }
      // Cache by (id, target, text-hash) when an id is supplied so re-clicks
      // and other visitors on the same entry don't re-spend neurons.
      // Including a hash of the text in the key prevents cache poisoning:
      // an attacker submitting different `text` under a real entry's `id`
      // can't overwrite what other visitors see, because the lookup hash
      // won't match what they're translating.
      let cacheKey = null;
      if (id) {
        // 128-bit prefix of SHA-256(text). 64 bits is collision-feasible for
        // an attacker who can submit translations (~2^32 work to land on
        // another entry's cache slot under the same id+target); 128 bits
        // raises that to ~2^64 which is firmly impractical.
        const hashBuf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
        const hashHex = Array.from(new Uint8Array(hashBuf, 0, 16))
          .map(b => b.toString(16).padStart(2, '0')).join('');
        cacheKey = 'translate:' + id + ':' + target + ':' + hashHex;
      }
      if (cacheKey) {
        const cached = await this.state.storage.get(cacheKey);
        if (typeof cached === 'string' && cached.length > 0) {
          return new Response(JSON.stringify({ translated: cached, cached: true }),
            { status: 200, headers: { 'Content-Type': 'application/json', ...corsHdrs } });
        }
      }

      let aiResp;
      try {
        aiResp = await this.env.AI.run('@cf/meta/m2m100-1.2b', {
          text,
          source_lang: source || 'en',
          target_lang: target.split('-')[0]   // m2m100 takes 2-letter codes
        });
      } catch (e) {
        console.error('translate AI.run failed:', e && e.message);
        return new Response('Translation backend error',
          { status: 502, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }
      const rawTranslated = aiResp && typeof aiResp.translated_text === 'string'
        ? aiResp.translated_text : '';
      // Defensive strip of HTML-significant characters before storing/serving.
      // Frontends use textContent so this isn't a primary defense, but a
      // model that echoes injected `<script>` markers shouldn't get a chance
      // to be rendered as HTML by any future consumer.
      const translated = rawTranslated.replace(/[<>]/g, '');
      if (!translated) {
        return new Response('Translation returned empty result',
          { status: 502, headers: { 'Content-Type': 'text/plain', ...corsHdrs } });
      }
      // Cache fire-and-forget; missing cache write isn't user-visible.
      if (cacheKey) {
        this.state.storage.put(cacheKey, translated).catch(() => {});
      }
      return new Response(JSON.stringify({ translated, cached: false }),
        { status: 200, headers: { 'Content-Type': 'application/json', ...corsHdrs } });
    }

    if (url.pathname === '/status-wide.svg') {
      const svg = this.buildStatusWide();
      return new Response(svg, {
        status: 200,
        headers: {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          'Cache-Control': 'public, max-age=60',
          ...SEC_HEADERS
        }
      });
    }

    // SSE fanout: browsers connect here and receive push updates the ESP sends via WS
    if (url.pathname === '/_stream') {
      // Cap concurrent SSE writers. Without this a flood could hoard DO memory (each writer
      // holds stream buffer state). 503 tells well-behaved clients to back off and retry.
      if (this.sseClients.size >= SSE_MAX_CLIENTS) {
        return new Response('Too many live connections, try again shortly', {
          status: 503,
          headers: { 'Content-Type': 'text/plain', 'Retry-After': '30', ...SEC_HEADERS }
        });
      }
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sseClients.add(writer);
      // make sure the alarm is ticking so dead-client sweeps run
      this._ensureAlarm(30000);
      // immediately send the most recent cached stats so new clients don't wait 5s.
      // Inject fresh clients count (same pattern as the broadcast/cache paths)
      // so the very first stats event a new viewer receives populates the
      // live-presence indicator. Without this, the indicator stayed hidden
      // until the next 15s ESP push.
      if (this.lastStats) {
        let body = this.lastStats;
        try {
          const parsed = JSON.parse(this.lastStats);
          parsed.clients = this.sseClients.size;
          body = JSON.stringify(parsed);
        } catch (e) { /* fall through to raw */ }
        const payload = new TextEncoder().encode(`event: stats\ndata: ${body}\n\n`);
        writer.write(payload).catch(() => {
          this.sseClients.delete(writer);
          writer.abort().catch(() => {});
        });
      } else {
        // send a zero-length comment so the connection is established promptly
        writer.write(new TextEncoder().encode(': connected\n\n')).catch(() => {
          this.sseClients.delete(writer);
          writer.abort().catch(() => {});
        });
      }
      return new Response(readable, {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-store',
          'X-Accel-Buffering': 'no',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/_ws' && request.headers.get('Upgrade') === 'websocket') {
      const wsIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const now = Date.now();
      let wf = this.wsAuthFails.get(wsIP);
      if (wf && now < wf.blockedUntil) {
        return new Response('Too many failed attempts', { status: 429 });
      }

      const key = url.searchParams.get('key');
      if (!key || !timingSafeEqualStr(key, this.env.WORKER_SECRET || '')) {
        if (!wf) wf = { count: 0, firstAt: now, blockedUntil: 0 };
        if (now - wf.firstAt > 60000) { wf.count = 0; wf.firstAt = now; }
        wf.count++;
        if (wf.count >= 5) wf.blockedUntil = now + 600000;
        this.wsAuthFails.set(wsIP, wf);
        return new Response('Unauthorized', { status: 403 });
      }
      this.wsAuthFails.delete(wsIP);

      // opportunistic cleanup of stale auth-fail entries (mirrors rateLimits cleanup)
      if (this.wsAuthFails.size > 500) {
        for (const [k, v] of this.wsAuthFails) {
          if (now - v.firstAt > 600000) this.wsAuthFails.delete(k);
        }
      }

      // Valid auth; take over any existing socket (covers stale sockets from
      // unclean reboots). We must explicitly fail any in-flight requests
      // here: the old socket's close-listener (line ~1925) short-circuits
      // when `this.espSocket !== oldServer`, which becomes true the moment
      // we null/replace this.espSocket below. So the listener won't drain
      // pendingRequests on its own and they'd hang for the 30s timeout.
      if (this.espSocket) {
        if (this.pendingRequests && this.pendingRequests.size) {
          for (const p of this.pendingRequests.values()) {
            try { p.resolve(offlineResponse()); } catch (e) {}
          }
          this.pendingRequests.clear();
        }
        try { this.espSocket.close(); } catch (e) {}
        this.espSocket = null;
      }
      // reset auth flag; each new connection must pass HMAC (or fall back if no HMAC_SECRET)
      this.hmacAuthenticated = false;
      if (this.pendingAuth) {
        clearTimeout(this.pendingAuth.timer);
        this.pendingAuth = null;
      }
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.espSocket = server;
      this.lastActivity = Date.now();

      // Optional HMAC handshake. If HMAC_SECRET is set, require the ESP to
      // respond to an auth_challenge before trusting the connection.
      if (this.env.HMAC_SECRET) {
        const nonce = crypto.randomUUID();
        this.pendingAuth = {
          nonce,
          socket: server,
          timer: setTimeout(() => {
            if (this.pendingAuth && this.pendingAuth.socket === server) {
              console.error('HMAC auth timeout');
              try { server.close(); } catch (e) {}
              this.pendingAuth = null;
              if (this.espSocket === server) this.espSocket = null;
            }
          }, 5000)
        };
        server.send(JSON.stringify({ type: 'auth_challenge', nonce }));
      } else {
        console.warn('HMAC_SECRET unset; ESP WS auth is WORKER_SECRET-only (development mode).');
        this.hmacAuthenticated = true;
      }

      server.addEventListener('message', (event) => {
        // Only count activity once HMAC is verified. Pre-auth, an attacker
        // hammering the WS upgrade could indefinitely refresh this timestamp
        // and silently mask the deadman alert on a truly-dead device.
        if (this.hmacAuthenticated) this.lastActivity = Date.now();
        const data = event.data;
        if (typeof data !== 'string') return;

        // HMAC auth response. Parse first (cheap, bounded by WS frame size), then
        // type-check; avoids relying on a substring match to decide whether to parse.
        if (this.pendingAuth && this.pendingAuth.socket === server && data.charAt(0) === '{') {
          let authMsg = null;
          try { authMsg = JSON.parse(data); } catch (e) { /* not JSON; fall through */ }
          if (authMsg && authMsg.type === 'auth_response') {
            const nonce = this.pendingAuth.nonce;
            if (!authMsg.hmac) {
              try { server.send(JSON.stringify({ type: 'auth_result', ok: false, reason: 'missing hmac' })); } catch (e) {}
              try { server.close(); } catch (e) {}
              this.espSocket = null;
              clearTimeout(this.pendingAuth.timer);
              this.pendingAuth = null;
              return;
            }
            this.verifyHmac(authMsg.hmac, nonce).then((valid) => {
              if (this.espSocket !== server) return;
              if (!valid) {
                console.error('HMAC mismatch');
                try { server.send(JSON.stringify({ type: 'auth_result', ok: false, reason: 'hmac mismatch' })); } catch (e) {}
                try { server.close(); } catch (e) {}
                this.espSocket = null;
              } else {
                this.hmacAuthenticated = true;
              }
              if (this.pendingAuth && this.pendingAuth.socket === server) {
                clearTimeout(this.pendingAuth.timer);
                this.pendingAuth = null;
              }
            }).catch((e) => {
              console.error('verifyHmac error:', e && e.message);
              if (this.espSocket === server) {
                try { server.close(); } catch (e2) {}
                this.espSocket = null;
              }
              if (this.pendingAuth && this.pendingAuth.socket === server) {
                clearTimeout(this.pendingAuth.timer);
                this.pendingAuth = null;
              }
            });
            return;
          }
        }
        // Reject all other messages until HMAC verified
        if (!this.hmacAuthenticated) return;

        if (data.length === 0) {
          if (this.currentStreamId === null) return;
          const ar = this.activeResponses.get(this.currentStreamId);
          if (!ar) { this.currentStreamId = null; return; }

          const totalLen = ar.chunks.reduce((s, c) => s + c.length, 0);
          const assembled = new Uint8Array(totalLen);
          let offset = 0;
          for (const chunk of ar.chunks) {
            assembled.set(chunk, offset);
            offset += chunk.length;
          }

          const headers = { 'Content-Type': ar.ct };
          if (ar.cc) headers['Cache-Control'] = ar.cc;
          if (ar.ce) headers['Content-Encoding'] = ar.ce;

          const pending = this.pendingRequests.get(ar.id);
          if (pending) {
            // encodeBody: 'manual' is required when ar.ce is set (gzipped
            // body) so the Workers runtime ships the bytes as-is without
            // re-encoding/decompressing on outer-worker arrayBuffer() reads.
            // For plain bodies the default ('auto') is fine and stays in
            // the streaming code path.
            const respInit = ar.ce
              ? { status: ar.status, headers, encodeBody: 'manual' }
              : { status: ar.status, headers };
            pending.resolve(new Response(assembled.buffer, respInit));
            this.pendingRequests.delete(ar.id);
          }
          this.activeResponses.delete(this.currentStreamId);
          this.currentStreamId = null;
          return;
        }

        if (data.charAt(0) === '{') {
          try {
            const msg = JSON.parse(data);
            if (msg.type === 'event') {
              this.handleEvent(msg).catch((e) => console.error('handleEvent error:', e && e.message));
              return;
            }
            // Drop metadata for IDs we never asked for. Without this guard, a
            // buggy or replayed frame could accumulate orphan entries in
            // activeResponses that only get cleared on full WS close.
            if (typeof msg.id !== 'number' || !this.pendingRequests.has(msg.id)) {
              console.warn('dropping stream metadata for unknown id:', msg.id);
              return;
            }
            this.activeResponses.set(msg.id, {
              id: msg.id,
              status: msg.status || 200,
              ct: msg.ct || 'text/html',
              cc: msg.cc || '',
              ce: msg.ce || '',
              chunks: []
            });
            this.currentStreamId = msg.id;
            return;
          } catch (e) {
            console.warn('malformed stream metadata frame:', e && e.message);
          }
        }

        if (this.currentStreamId !== null) {
          const ar = this.activeResponses.get(this.currentStreamId);
          if (ar) {
            try {
              ar.chunks.push(b64ToBytes(data));
            } catch (e) {
              console.error('b64 decode error:', e && e.message);
            }
          }
        }
      });

      server.addEventListener('close', () => {
        // Only clear state if THIS socket is still the active one.
        // A takeover reconnect may have already replaced us with a new socket.
        if (this.espSocket !== server) return;
        this.espSocket = null;
        this.hmacAuthenticated = false;
        if (this.pendingAuth && this.pendingAuth.socket === server) {
          clearTimeout(this.pendingAuth.timer);
          this.pendingAuth = null;
        }
        for (const p of this.pendingRequests.values()) {
          p.resolve(offlineResponse());
        }
        this.pendingRequests.clear();
        this.activeResponses.clear();
        this.currentStreamId = null;
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // maintenance window takes precedence over offline, so planned work shows the right page
    if (Date.now() < this.maintenanceUntil) {
      return maintenanceResponse(this.maintenanceUntil, this.maintenanceMessage);
    }

    if (!this.espSocket || !this.hmacAuthenticated) return offlineResponse();

    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const limited = this._enforceRateLimit(clientIP);
    if (limited) return limited;

    const id = ++this.requestId;

    // request.cf.country is the canonical source inside a Worker; the CF-IPCountry HTTP header
    // isn't forwarded to Workers by default, so reading it via headers.get always returned '' and
    // every relayed request showed up as ?? in the console.
    const headers = {
      'CF-Connecting-IP': clientIP,
      'CF-IPCountry': (request.cf && request.cf.country) || request.headers.get('CF-IPCountry') || '',
      'Accept-Encoding': request.headers.get('Accept-Encoding') || ''
    };

    let body = '';
    if (request.method === 'POST') {
      const cl = request.headers.get('Content-Length');
      if (cl && parseInt(cl, 10) > MAX_BODY) {
        return new Response('Payload too large', { status: 413, headers: { 'Content-Type': 'text/plain', ...SEC_HEADERS } });
      }
      // Stream the body so a missing or lying Content-Length can't slip a huge
      // payload through and then get fully buffered by request.text(). Cancel
      // the stream the moment we exceed MAX_BODY. fatal:true on the decoder
      // throws on non-UTF-8 input rather than silently substituting U+FFFD,
      // since the WS relay frame embeds body as a JSON string and any binary
      // POST would arrive at the ESP corrupted.
      if (request.body) {
        const reader = request.body.getReader();
        const decoder = new TextDecoder('utf-8', { fatal: true });
        let received = 0;
        let parts = '';
        let tooLarge = false;
        let decodeFailed = false;
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            received += value.byteLength;
            if (received > MAX_BODY) {
              tooLarge = true;
              try { await reader.cancel(); } catch (e) {}
              break;
            }
            parts += decoder.decode(value, { stream: true });
          }
          if (!tooLarge) parts += decoder.decode();
        } catch (e) {
          decodeFailed = true;
          try { await reader.cancel(); } catch (err) {}
        }
        if (tooLarge) {
          return new Response('Payload too large', { status: 413, headers: { 'Content-Type': 'text/plain', ...SEC_HEADERS } });
        }
        if (decodeFailed) {
          return new Response('Binary payloads not supported via Worker relay', { status: 415, headers: { 'Content-Type': 'text/plain', ...SEC_HEADERS } });
        }
        body = parts;
      }
    }

    try {
      this.espSocket.send(JSON.stringify({
        id,
        method: request.method,
        path: url.pathname + url.search,
        headers,
        body
      }));
    } catch (e) {
      this.espSocket = null;
      return offlineResponse();
    }

    // Tag /stats relay responses so we can enrich + cache them on the way
    // back. Without this, a cold-worker /stats falls through to the raw ESP
    // body (no `outdoor` weather block), and the homepage flickers the
    // outdoor section on/off until lastStats warms back up. Catching the
    // first relayed /stats here populates lastStats so all subsequent polls
    // are consistent.
    const isStatsPath = (url.pathname === '/stats' && request.method === 'GET');

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        this.activeResponses.delete(id);
        if (this.currentStreamId === id) this.currentStreamId = null;
        resolve(timeoutResponse());
      }, 30000);

      this.pendingRequests.set(id, {
        resolve: async (resp) => {
          clearTimeout(timeout);
          if (isStatsPath && resp && resp.status === 200) {
            try {
              const cloned = resp.clone();
              const raw = await cloned.json();
              const enriched = this.enrichStats(raw);
              this.lastStats = JSON.stringify(enriched);
              this.lastStatsAt = Date.now();
              // Inject live presence count for this response (kept out of
              // lastStats itself so subsequent serves get the current count).
              const responseBody = JSON.stringify({ ...enriched, clients: this.sseClients.size });
              resolve(new Response(responseBody, {
                status: 200,
                headers: {
                  'Content-Type': 'application/json',
                  'Cache-Control': 'public, max-age=5, stale-while-revalidate=30',
                  ...SEC_HEADERS
                }
              }));
              return;
            } catch (e) {
              // Fall through to raw response on parse error
            }
          }
          resolve(resp);
        }
      });
    });
  }
}

// Prefix matches: covers path families like /admin, /admin/files, etc.
const NO_CACHE_PREFIX = ['/logs', '/admin', '/_ws', '/_stream',
  '/guestbook/entries', '/guestbook/submit', '/guestbook/pending', '/guestbook/moderate',
  '/guestbook/replies', '/guestbook/locate'];
// Exact matches: prefix would over-match (e.g. /snake/seed vs /snake/seedling).
// /snake/leaderboard intentionally cacheable via its own Cache-Control.
const NO_CACHE_EXACT = new Set(['/console.json', '/snake/seed', '/snake/score',
  '/guestbook/translate']);

function shouldCache(pathname) {
  if (pathname === '/stats') return false;
  if (NO_CACHE_EXACT.has(pathname)) return false;
  return !NO_CACHE_PREFIX.some(p => pathname.startsWith(p));
}

// CLI clients on "/" bypass the edge cache so browsers don't get the ASCII
// card and CLIs don't get the cached HTML. The DO sends Cache-Control:
// no-store on the card response so the inverse poison can't happen either.
function isCliRoot(request, url) {
  if (request.method !== 'GET') return false;
  if (url.pathname !== '/' && url.pathname !== '') return false;
  const ua = (request.headers.get('User-Agent') || '').toLowerCase();
  return /\b(curl|wget|httpie|libwww-perl|powershell)\b/.test(ua);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const cliRoot = isCliRoot(request, url);
    const cacheable = !cliRoot && request.method === 'GET' && shouldCache(url.pathname);

    if (cacheable) {
      const cached = await caches.default.match(request);
      if (cached) return cached;
    }

    const id = env.ESP_RELAY.idFromName('main');
    const relay = env.ESP_RELAY.get(id);
    const response = await relay.fetch(request);

    // 101 WebSocket upgrades must be returned as-is (can't reconstruct or add headers)
    if (response.status === 101) return response;

    if (cacheable && (response.status === 200 || response.status === 404)) {
      const cc = response.headers.get('Cache-Control');
      if (cc && cc.includes('max-age')) {
        // encodeBody: 'manual' when Content-Encoding is set (gzip) so the
        // runtime doesn't auto-decompress on arrayBuffer(). The cached body
        // stays in its original gzip form and the Content-Encoding header
        // on the cached Response correctly describes those bytes.
        const ce = response.headers.get('Content-Encoding');
        const body = await response.arrayBuffer();
        const headers = applySecHeaders(new Headers(response.headers));
        headers.set('Cache-Control', cc);

        const cacheInit = ce
          ? { status: response.status, headers, encodeBody: 'manual' }
          : { status: response.status, headers };
        const cacheResp = new Response(body, cacheInit);
        await caches.default.put(request, cacheResp.clone());
        return cacheResp;
      }
    }

    const passHeaders = applySecHeaders(new Headers(response.headers));
    const passCe = response.headers.get('Content-Encoding');
    const passInit = passCe
      ? { status: response.status, headers: passHeaders, encodeBody: 'manual' }
      : { status: response.status, headers: passHeaders };
    return new Response(response.body, passInit);
  }
};
