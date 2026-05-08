// HelloESP Cloudflare Worker relay (WebSocket + chunked base64 streaming)

const PAGE_CSS = `*{margin:0;padding:0;box-sizing:border-box}:root{--bg:#f8f7f4;--ink:#1a1a1a;--mid:#888;--faint:#ccc}@media(prefers-color-scheme:dark){:root{--bg:#111110;--ink:#e8e6e1;--mid:#8a8a87;--faint:#2a2a28}}body{background:var(--bg);color:var(--ink);font-family:ui-monospace,"SF Mono","Cascadia Mono","Consolas",monospace;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:24px}main{margin:auto 0}main{max-width:480px;text-align:center}h1{font-size:clamp(48px,10vw,72px);font-weight:400;letter-spacing:-0.02em;margin-bottom:20px}p{font-size:13px;color:var(--mid);line-height:1.8;margin-bottom:16px}p.lede{color:var(--ink);font-size:14px;margin-bottom:24px}.note{font-size:11px;color:var(--mid);border-top:1px solid var(--faint);padding-top:20px;margin-top:28px;line-height:1.7}a{color:var(--ink);font-size:11px;letter-spacing:0.1em;text-transform:uppercase;text-underline-offset:3px;display:inline-block;margin:0 8px}.site-name{display:block;font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:var(--mid);text-decoration:none;margin:0 0 8px}.site-name:hover{color:var(--ink)}.status{font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:var(--mid);margin-top:28px}.dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--mid);margin-left:6px;vertical-align:middle;animation:pulse 1.4s ease-in-out infinite}@keyframes pulse{0%,100%{opacity:0.25}50%{opacity:1}}a:focus-visible{outline:2px solid var(--ink);outline-offset:2px;border-radius:2px}.action{color:var(--ink);font-weight:600}.helper{font-size:10px;color:var(--mid);letter-spacing:0.08em;text-transform:uppercase;margin-top:6px}@media(prefers-reduced-motion:reduce){.dot{animation:none}}`;

const CHIP_ICON_PATHS = `<path fill-rule="evenodd" d="M7 5a2 2 0 00-2 2v18a2 2 0 002 2h18a2 2 0 002-2V7a2 2 0 00-2-2H7zm6 7a1 1 0 00-1 1v6a1 1 0 001 1h6a1 1 0 001-1v-6a1 1 0 00-1-1h-6z"/><rect x="7.5" y="1" width="2" height="3" rx=".5"/><rect x="12.5" y="1" width="2" height="3" rx=".5"/><rect x="17.5" y="1" width="2" height="3" rx=".5"/><rect x="22.5" y="1" width="2" height="3" rx=".5"/><rect x="7.5" y="28" width="2" height="3" rx=".5"/><rect x="12.5" y="28" width="2" height="3" rx=".5"/><rect x="17.5" y="28" width="2" height="3" rx=".5"/><rect x="22.5" y="28" width="2" height="3" rx=".5"/><rect x="1" y="7.5" width="3" height="2" rx=".5"/><rect x="1" y="12.5" width="3" height="2" rx=".5"/><rect x="1" y="17.5" width="3" height="2" rx=".5"/><rect x="1" y="22.5" width="3" height="2" rx=".5"/><rect x="28" y="7.5" width="3" height="2" rx=".5"/><rect x="28" y="12.5" width="3" height="2" rx=".5"/><rect x="28" y="17.5" width="3" height="2" rx=".5"/><rect x="28" y="22.5" width="3" height="2" rx=".5"/>`;

const SNAKE_GAME = `<div class="snk" role="group" aria-label="Snake"><div class="snk-h"><span class="snk-h-l">Snake</span><div class="snk-h-r"><span class="snk-stat"><span class="snk-stat-k">Score</span><b id="snk-s">0</b></span><span class="snk-stat snk-stat-pb" id="snk-pb-wrap" hidden><span class="snk-stat-k">Best</span><b id="snk-pb">0</b></span></div></div><div class="snk-board"><canvas id="snk-c" width="280" height="280" aria-hidden="true"></canvas><div class="snk-o" id="snk-o"><div class="snk-o-inner" id="snk-m"><div class="snk-go">Snake</div><div class="snk-cta">tap to play</div><div class="snk-hint"><span class="snk-hint-desk"><span class="snk-key">&uarr;</span><span class="snk-key">&larr;</span><span class="snk-key">&darr;</span><span class="snk-key">&rarr;</span> or <span class="snk-key">W</span><span class="snk-key">A</span><span class="snk-key">S</span><span class="snk-key">D</span></span><span class="snk-hint-touch">Swipe or tap arrows below</span></div></div></div></div><button type="button" class="snk-stop-replay" id="snk-stop-replay" hidden>Stop replay</button><div class="snk-p" aria-hidden="true"><button type="button" data-snk="up">&uarr;</button><button type="button" data-snk="left">&larr;</button><button type="button" data-snk="down">&darr;</button><button type="button" data-snk="right">&rarr;</button></div><div class="snk-lb" id="snk-lb-today-section" hidden><div class="snk-lb-h"><span class="snk-lb-h-t">Today</span><span class="snk-lb-h-line"></span><span class="snk-lb-h-hint">tap &#9654; to watch</span></div><ol class="snk-lb-l" id="snk-lb-today"></ol></div><div class="snk-lb" id="snk-lb-alltime-section" hidden><div class="snk-lb-h"><span class="snk-lb-h-t">All-time</span><span class="snk-lb-h-line"></span><span class="snk-lb-h-hint">tap &#9654; to watch</span></div><ol class="snk-lb-l" id="snk-lb-alltime"></ol></div></div><style>.snk{max-width:280px;margin:32px auto 0;text-align:left}.snk-board{position:relative}.snk-h{display:flex;justify-content:space-between;align-items:baseline;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:var(--mid);margin-bottom:8px}.snk-h-l{font-weight:500;color:var(--ink)}.snk-h-r{display:flex;gap:14px;align-items:baseline}.snk-stat{display:inline-flex;gap:6px;align-items:baseline}.snk-stat-k{font-size:9px;letter-spacing:0.12em;color:var(--mid)}.snk-stat b{color:var(--ink);font-weight:500;font-size:14px;letter-spacing:0.02em;font-variant-numeric:tabular-nums}.snk-stat-pb b{color:var(--mid)}#snk-c{display:block;width:100%;max-width:280px;aspect-ratio:1;background:var(--faint);border:1px solid var(--faint);border-radius:3px;image-rendering:pixelated;touch-action:none;cursor:crosshair;box-shadow:inset 0 0 0 1px rgba(0,0,0,0.06)}.snk-o{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.62);color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;padding:16px;text-align:center;border-radius:3px}.snk-o.hide{display:none}.snk-o-inner{max-width:240px}.snk-o .snk-go{font-size:22px;letter-spacing:-0.01em;text-transform:none;font-weight:400;color:#fff;margin-bottom:6px;line-height:1}.snk-o .snk-go-sc{font-size:36px;font-weight:300;letter-spacing:0;color:#fff;font-variant-numeric:tabular-nums;line-height:1;margin:8px 0 6px}.snk-o .snk-go-pb{font-size:9px;color:#bbb;letter-spacing:0.1em;margin-top:4px}.snk-o .snk-go-sub{font-size:9px;opacity:0.55;letter-spacing:0.12em;margin-top:14px}.snk-o .snk-cta{font-size:11px;color:#fff;opacity:0.85;letter-spacing:0.18em;text-transform:uppercase;margin-top:14px;animation:snk-pulse 2.4s ease-in-out infinite}.snk-o .snk-hint{font-size:9px;color:#fff;opacity:0.45;letter-spacing:0.1em;margin-top:10px;text-transform:uppercase}.snk-hint-touch{display:none}@media(any-pointer:coarse),(max-width:600px){.snk-hint-desk{display:none}.snk-hint-touch{display:inline}}.snk-add-lb{font:inherit;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;padding:7px 14px;background:transparent;color:#fff;border:1px solid #aaa;border-radius:3px;cursor:pointer;margin-top:12px;transition:border-color 0.1s,background 0.1s}.snk-add-lb:hover{border-color:#fff;background:rgba(255,255,255,0.08)}.snk-p{display:none;grid-template-columns:repeat(3,52px);grid-template-rows:repeat(2,52px);gap:4px;justify-content:center;margin:12px auto 0}.snk-p button{background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:20px;border-radius:4px;touch-action:manipulation;cursor:pointer;transition:color 0.1s,border-color 0.1s}.snk-p button:hover,.snk-p button:active{color:var(--ink);border-color:var(--mid)}.snk-p [data-snk=up]{grid-column:2;grid-row:1}.snk-p [data-snk=left]{grid-column:1;grid-row:2}.snk-p [data-snk=down]{grid-column:2;grid-row:2}.snk-p [data-snk=right]{grid-column:3;grid-row:2}@media(any-pointer:coarse),(max-width:600px){.snk-p{display:grid}}.snk-key{display:inline-block;border:1px solid currentColor;border-radius:3px;padding:1px 4px;font-size:9px;line-height:1;margin:0 1px;font-family:ui-monospace,monospace;min-width:8px;text-align:center;letter-spacing:0}.snk-lb{margin-top:18px;font-size:11px;color:var(--mid)}.snk-lb-h{display:flex;align-items:center;gap:8px;margin-bottom:8px}.snk-lb-h-t{text-transform:uppercase;letter-spacing:0.15em;font-size:9px;color:var(--mid);white-space:nowrap}.snk-lb-h-line{flex:1;height:1px;background:var(--faint)}.snk-lb-h-hint{font-size:9px;color:var(--mid);text-transform:none;letter-spacing:0;opacity:0.75;white-space:nowrap;font-style:italic}.snk-lb-l{list-style:none;padding:0;margin:0;font-variant-numeric:tabular-nums}.snk-lb-l li{display:grid;grid-template-columns:20px 42px 1fr auto;gap:8px;padding:3px 4px;align-items:baseline;border-radius:2px;margin-left:-4px;margin-right:-4px}.snk-lb-l .ri{color:var(--mid);text-align:right;font-size:10px;font-weight:500}.snk-lb-l .ii{color:var(--ink);letter-spacing:0.1em;font-size:11px}.snk-lb-l .sc{color:var(--ink);font-weight:500}.snk-lb-l .dt{color:var(--mid);font-size:9px;letter-spacing:0.05em}.snk-lb-l li.r1 .ri{color:#c89b1a}.snk-lb-l li.r2 .ri{color:#9da3ac}.snk-lb-l li.r3 .ri{color:#b87333}.snk-lb-l li.r1{background:rgba(200,155,26,0.07)}.snk-lb-l li.you{outline:1px solid var(--mid)}.snk-lb-l li.you .ii::before{content:'> ';color:var(--mid);letter-spacing:0}.snk-lb-empty{font-size:10px;color:var(--mid);font-style:italic;list-style:none;padding:6px 0;text-align:center}.snk-init-form{display:flex;flex-direction:column;gap:10px;align-items:center;font-family:inherit;text-transform:none;letter-spacing:0;margin-top:12px}.snk-init-headline{font-size:11px;letter-spacing:0.25em;color:#ffd24a;text-transform:uppercase;animation:snk-pulse 1.4s ease-in-out infinite}@keyframes snk-pulse{0%,100%{opacity:0.6}50%{opacity:1}}.snk-init-form input{font:inherit;font-size:24px;letter-spacing:0.5em;text-align:center;text-transform:uppercase;width:120px;padding:8px 6px 8px 12px;background:transparent;border:1px solid #888;color:#fff;border-radius:3px;outline:none}.snk-init-form input:focus{border-color:#fff}.snk-init-form button{font:inherit;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;padding:8px 18px;background:#fff;color:#000;border:none;border-radius:3px;cursor:pointer;transition:background 0.1s}.snk-init-form button:hover{background:#e8e6e1}.snk-init-form button[disabled]{opacity:0.5;cursor:default}.snk-init-err{font-size:10px;color:#ff7a7a;letter-spacing:0.05em;text-transform:none;min-height:14px;text-align:center;font-family:inherit;margin-top:-2px}.snk-init-form.snk-err input{border-color:#ff7a7a}.snk-init-form.snk-shake{animation:snk-shake 0.32s ease-in-out}@keyframes snk-shake{0%,100%{transform:translateX(0)}25%{transform:translateX(-5px)}75%{transform:translateX(5px)}}.snk-init-skip{font-size:9px;opacity:0.55;letter-spacing:0.12em;margin-top:2px}@media(prefers-reduced-motion:reduce){.snk-init-headline{animation:none;opacity:0.9}.snk-o .snk-cta{animation:none}.snk-init-form.snk-shake{animation:none}}.snk-lb-l li{grid-template-columns:20px 42px 1fr auto auto}.snk-watch{background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:9px;padding:2px 6px;cursor:pointer;border-radius:2px;line-height:1;transition:color 0.1s,border-color 0.1s}.snk-watch:hover{color:var(--ink);border-color:var(--mid)}.snk-watch.invisible{visibility:hidden;pointer-events:none}.snk-stop-replay{display:block;margin:8px auto 0;background:none;border:1px solid var(--faint);color:var(--mid);font:inherit;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;padding:4px 12px;cursor:pointer;border-radius:2px;transition:color 0.1s,border-color 0.1s}.snk-stop-replay[hidden]{display:none}.snk-stop-replay:hover{color:var(--ink);border-color:var(--mid)}</style><script>(function(){var c=document.getElementById('snk-c'),ov=document.getElementById('snk-o'),mg=document.getElementById('snk-m'),sc=document.getElementById('snk-s');if(!c)return;var pbWrap=document.getElementById('snk-pb-wrap'),pbEl=document.getElementById('snk-pb');var cx=c.getContext('2d'),CELL=14,COLS=20,ROWS=20,state='idle',sn,dr,nd,fd,score,ls,ms,stepN,moves,gameId,seed,rngState,showingInit=false,lastTodayRank=null,lastAlltimeRank=null,personalBest=0,pendingSubmit=null,demoTimer=null;var todayEl=document.getElementById('snk-lb-today-section'),todayList=document.getElementById('snk-lb-today'),alltimeEl=document.getElementById('snk-lb-alltime-section'),alltimeList=document.getElementById('snk-lb-alltime'),todayBoard=[],alltimeBoard=[];var API=(location.host==='helloesp.com'||location.host==='www.helloesp.com')?'':'https://helloesp.com';try{var pb=parseInt(localStorage.getItem('snk-pb')||'0',10);if(pb>0){personalBest=pb;pbEl.textContent=pb;pbWrap.hidden=false;}}catch(e){}function esc(s){return String(s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}function fmtDate(t){if(!t)return '';try{return new Date(t*1000).toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'});}catch(e){return '';}}function fmtIso(t){if(!t)return '';try{return new Date(t*1000).toISOString();}catch(e){return '';}}function renderBoard(listEl,sectionEl,data,isAlltime){if(!listEl||!sectionEl)return;if(!data||!data.length){listEl.innerHTML='<li class="snk-lb-empty">No scores yet. Be first.</li>';sectionEl.hidden=false;return;}var youRank=isAlltime?lastAlltimeRank:lastTodayRank;var html='';for(var i=0;i<data.length;i++){var e=data[i];var rankCls=i<3?' r'+(i+1):'';var youCls=(youRank===i+1)?' you':'';html+='<li class="'+(rankCls+youCls).trim()+'"><span class="ri">'+(i+1)+'</span><span class="ii">'+esc(e.i)+'</span><span class="sc">'+e.s+'</span><span class="dt" title="'+esc(fmtIso(e.t))+'">'+esc(fmtDate(e.t))+'</span><button class="snk-watch'+(e.g?'':' invisible')+'" data-g="'+(e.g?esc(e.g):'')+'" aria-label="Watch replay" title="Watch replay" '+(e.g?'':'tabindex="-1" aria-hidden="true"')+'>&#9654;</button></li>';}listEl.innerHTML=html;sectionEl.hidden=false;}function renderBoth(){renderBoard(todayList,todayEl,todayBoard,false);renderBoard(alltimeList,alltimeEl,alltimeBoard,true);}function fetchBoth(){Promise.all([fetch(API+'/snake/daily/leaderboard',{cache:'no-cache'}).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;}),fetch(API+'/snake/leaderboard',{cache:'no-cache'}).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;})]).then(function(results){var todayResp=results[0],altResp=results[1];todayBoard=(todayResp&&Array.isArray(todayResp.leaderboard))?todayResp.leaderboard:[];alltimeBoard=Array.isArray(altResp)?altResp:[];renderBoth();});}function requestSeed(cb){fetch(API+'/snake/seed',{cache:'no-store'}).then(function(r){return r.ok?r.json():null;}).then(function(d){if(d&&typeof d.seed==='number'&&typeof d.gameId==='string'){seed=d.seed;gameId=d.gameId;}cb();}).catch(function(){cb();});}function rng32(s){s=(s+0x6D2B79F5)|0;var t=s;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return [(((t^(t>>>14))>>>0)/4294967296),s];}function sf(){if(seed!=null){for(var i=0;i<200;i++){var r1=rng32(rngState);rngState=r1[1];var r2=rng32(rngState);rngState=r2[1];var x=Math.floor(r1[0]*COLS),y=Math.floor(r2[0]*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}else{for(var k=0;k<400;k++){var x=Math.floor(Math.random()*COLS),y=Math.floor(Math.random()*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}}function reset(){sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};nd=dr;score=0;ms=140;stepN=0;moves=[];rngState=(seed!=null?seed:0)|0;sf();sc.textContent=0;draw();}function step(){if(nd.x!==-dr.x||nd.y!==-dr.y)dr=nd;var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};stepN++;if(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS)return over();if(sn.some(function(s){return s.x===h.x&&s.y===h.y}))return over();sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){score+=10;sc.textContent=score;sf();if(ms>65)ms-=3;}else sn.pop();draw();}function draw(){cx.clearRect(0,0,c.width,c.height);cx.fillStyle=(state==='demo')?'#c97326':'#e67e22';cx.fillRect(fd.x*CELL+2,fd.y*CELL+2,CELL-4,CELL-4);cx.fillStyle=(state==='demo')?'#5a8db9':'#2686e6';for(var i=0;i<sn.length;i++){var s=sn[i];cx.fillRect(s.x*CELL+1,s.y*CELL+1,CELL-2,CELL-2);}}function loop(t){if(state!=='playing')return;if(!ls)ls=t;if(t-ls>=ms){ls=t;step();}if(state==='playing')requestAnimationFrame(loop);}function startGame(){if(state==='playing')return;stopDemo();reset();state='playing';ls=0;ov.classList.add('hide');requestAnimationFrame(loop);}function play(){pendingSubmit=null;lastAlltimeRank=null;lastTodayRank=null;if(state==='paused'){state='playing';ls=0;ov.classList.add('hide');requestAnimationFrame(loop);return;}if(state==='over'){seed=null;gameId=null;}if(seed==null){requestSeed(startGame);return;}startGame();}function pause(){if(state!=='playing')return;state='paused';mg.innerHTML='<div class="snk-go">Paused</div><div class="snk-go-sub">tap to resume</div>';ov.classList.remove('hide');}function updatePB(){if(score>personalBest){personalBest=score;try{localStorage.setItem('snk-pb',String(score));}catch(e){}pbEl.textContent=score;pbWrap.hidden=false;}}function over(){state='over';updatePB();var base=score>=10&&gameId!=null;var qT=base&&(todayBoard.length<10||score>(todayBoard[todayBoard.length-1].s||0));var qA=base&&(alltimeBoard.length<10||score>(alltimeBoard[alltimeBoard.length-1].s||0));if(qT||qA){pendingSubmit={score:score,gameId:gameId,moves:moves.slice(),seed:seed};showInitials(false);}else{pendingSubmit=null;showOver();}}function showOver(){showingInit=false;var pbLine=personalBest>0?'<div class="snk-go-pb">Your best &bull; '+personalBest+'</div>':'';var altLine=lastAlltimeRank?'<div class="snk-go-pb" style="color:#ffd24a;">All-time rank &bull; #'+lastAlltimeRank+'</div>':'';var addBtn=pendingSubmit?'<button type="button" class="snk-add-lb" id="snk-add-lb">Add to leaderboard</button>':'';mg.innerHTML='<div class="snk-go">Game over</div><div class="snk-go-sc">'+score+'</div>'+pbLine+altLine+addBtn+'<div class="snk-go-sub">tap canvas to play again</div>';ov.classList.remove('hide');if(pendingSubmit){var addB=document.getElementById('snk-add-lb');if(addB)addB.addEventListener('click',function(e){e.stopPropagation();showInitials(true);});}}function showInitials(isReopen){showingInit=true;var sChk=pendingSubmit?pendingSubmit.score:score;var altQ=alltimeBoard.length<10||sChk>(alltimeBoard[alltimeBoard.length-1].s||0);var head=isReopen?'Add your initials':(altQ?'New high score':'New daily high score');var displayScore=pendingSubmit?pendingSubmit.score:score;mg.innerHTML='<div class="snk-init-headline">'+head+'</div><div class="snk-go-sc">'+displayScore+'</div><form class="snk-init-form" id="snk-init-f"><input id="snk-init-i" maxlength="3" pattern="[A-Za-z0-9]{3}" placeholder="AAA" autocomplete="off" autocapitalize="characters" inputmode="latin" required><button type="submit">Submit</button><div class="snk-init-err" id="snk-init-err"></div><span class="snk-init-skip">tap outside to skip</span></form>';ov.classList.remove('hide');var f=document.getElementById('snk-init-f'),inp=document.getElementById('snk-init-i');setTimeout(function(){if(inp)inp.focus();},20);f.addEventListener('submit',function(e){e.preventDefault();var v=(inp.value||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,3);if(v.length!==3){inp.focus();return;}submit(v);});}function submit(initials){if(!pendingSubmit)return;var btn=document.querySelector('#snk-init-f button'),inp=document.getElementById('snk-init-i'),err=document.getElementById('snk-init-err'),f=document.getElementById('snk-init-f');if(btn){btn.disabled=true;btn.textContent='...';}if(inp)inp.disabled=true;if(err)err.textContent='';if(f)f.classList.remove('snk-err','snk-shake');var payload={gameId:pendingSubmit.gameId,initials:initials,score:pendingSubmit.score,moves:pendingSubmit.moves};fetch(API+'/snake/score',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}).then(function(r){return r.json().catch(function(){return null;}).then(function(j){return {ok:r.ok,body:j};});}).then(function(res){if(res.ok&&res.body&&res.body.ok){if(res.body.today&&Array.isArray(res.body.today.board)){todayBoard=res.body.today.board;lastTodayRank=res.body.today.rank||null;}if(res.body.alltime&&Array.isArray(res.body.alltime.board)){alltimeBoard=res.body.alltime.board;lastAlltimeRank=(typeof res.body.alltime.rank==='number')?res.body.alltime.rank:null;}renderBoth();pendingSubmit=null;gameId=null;seed=null;showOver();}else{if(btn){btn.disabled=false;btn.textContent='Submit';}if(inp){inp.disabled=false;inp.value='';inp.focus();}var msg=(res.body&&res.body.error)||'submission failed';if(err)err.textContent=msg;if(f){f.classList.add('snk-err','snk-shake');setTimeout(function(){if(f)f.classList.remove('snk-shake');},340);}var fatalRe=/expired|not found|score does not match/i;if(res.body&&res.body.error&&fatalRe.test(res.body.error)){pendingSubmit=null;if(btn)btn.disabled=true;if(inp)inp.disabled=true;}}}).catch(function(){if(btn){btn.disabled=false;btn.textContent='Submit';}if(inp)inp.disabled=false;if(err)err.textContent='connection failed, try again';if(f){f.classList.add('snk-shake');setTimeout(function(){if(f)f.classList.remove('snk-shake');},340);}});}function setDir(x,y){if(state!=='playing')return;if(x===-dr.x&&y===-dr.y)return;if(nd.x===x&&nd.y===y)return;nd={x:x,y:y};var ch=x===0?(y<0?'U':'D'):(x<0?'L':'R');moves.push(stepN+ch);}function aiPick(){var head=sn[0],dx=fd.x-head.x,dy=fd.y-head.y,prefs=[];if(Math.abs(dx)>=Math.abs(dy)){if(dx>0)prefs.push([1,0]);else if(dx<0)prefs.push([-1,0]);if(dy>0)prefs.push([0,1]);else if(dy<0)prefs.push([0,-1]);}else{if(dy>0)prefs.push([0,1]);else if(dy<0)prefs.push([0,-1]);if(dx>0)prefs.push([1,0]);else if(dx<0)prefs.push([-1,0]);}var all=[[1,0],[-1,0],[0,1],[0,-1]];for(var i=0;i<all.length;i++){var found=false;for(var j=0;j<prefs.length;j++)if(prefs[j][0]===all[i][0]&&prefs[j][1]===all[i][1]){found=true;break;}if(!found)prefs.push(all[i]);}for(var k=0;k<prefs.length;k++){var mv=prefs[k];if(mv[0]===-dr.x&&mv[1]===-dr.y)continue;var nx=head.x+mv[0],ny=head.y+mv[1];if(nx<0||nx>=COLS||ny<0||ny>=ROWS)continue;var willGrow=(nx===fd.x&&ny===fd.y),hit=false;for(var l=0;l<sn.length-(willGrow?0:1);l++)if(sn[l].x===nx&&sn[l].y===ny){hit=true;break;}if(!hit)return mv;}return null;}function demoSpawnFood(){for(var k=0;k<400;k++){var x=Math.floor(Math.random()*COLS),y=Math.floor(Math.random()*ROWS);if(!sn.some(function(s){return s.x===x&&s.y===y})){fd={x:x,y:y};return;}}}function demoStart(){if(state==='playing'||state==='paused'||state==='over')return;state='demo';sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};demoSpawnFood();draw();demoTick();}function demoTick(){if(state!=='demo'){demoTimer=null;return;}var pick=aiPick();if(!pick){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},800);return;}dr={x:pick[0],y:pick[1]};var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};if(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS||sn.some(function(s){return s.x===h.x&&s.y===h.y})){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},800);return;}sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){demoSpawnFood();if(sn.length>=COLS*ROWS-3){demoTimer=setTimeout(function(){demoTimer=null;if(state==='demo')demoStart();},1200);draw();return;}}else sn.pop();draw();demoTimer=setTimeout(demoTick,170);}function stopDemo(){if(demoTimer){clearTimeout(demoTimer);demoTimer=null;}}var K={ArrowUp:[0,-1],ArrowDown:[0,1],ArrowLeft:[-1,0],ArrowRight:[1,0],w:[0,-1],s:[0,1],a:[-1,0],d:[1,0],W:[0,-1],S:[0,1],A:[-1,0],D:[1,0]};document.addEventListener('keydown',function(e){if(showingInit)return;if(!K[e.key])return;e.preventDefault();if(state!=='playing')play();else setDir(K[e.key][0],K[e.key][1]);});var ts=null;c.addEventListener('touchstart',function(e){ts={x:e.touches[0].clientX,y:e.touches[0].clientY};},{passive:true});c.addEventListener('touchmove',function(e){if(ts&&state==='playing')e.preventDefault();},{passive:false});c.addEventListener('touchend',function(e){if(!ts)return;var t=e.changedTouches[0],dx=t.clientX-ts.x,dy=t.clientY-ts.y;if(Math.abs(dx)<20&&Math.abs(dy)<20){ts=null;return;}if(state==='playing'){if(Math.abs(dx)>Math.abs(dy))setDir(dx>0?1:-1,0);else setDir(0,dy>0?1:-1);}else if(!showingInit){play();}ts=null;});document.querySelectorAll('.snk-p button').forEach(function(b){b.addEventListener('click',function(){if(showingInit)return;if(state!=='playing'){play();return;}var d=b.getAttribute('data-snk');if(d==='up')setDir(0,-1);else if(d==='down')setDir(0,1);else if(d==='left')setDir(-1,0);else if(d==='right')setDir(1,0);});});ov.addEventListener('click',function(e){if(e.target.closest&&e.target.closest('#snk-init-f'))return;if(e.target.closest&&e.target.closest('#snk-add-lb'))return;if(showingInit){showOver();return;}if(state!=='playing')play();});var prefersReducedMotion=function(){try{return window.matchMedia&&window.matchMedia('(prefers-reduced-motion: reduce)').matches;}catch(e){return false;}};document.addEventListener('visibilitychange',function(){if(document.hidden){if(state==='playing')pause();if(state==='demo')stopDemo();}else{if((state==='idle'||state==='demo')&&!prefersReducedMotion())demoStart();}});function fetchAndPlayReplay(gid){if(state==='playing'||state==='paused')return;fetch(API+'/snake/replay?gameId='+encodeURIComponent(gid),{cache:'force-cache'}).then(function(r){return r.ok?r.json():null;}).then(function(d){if(!d||typeof d.seed!=='number'||!Array.isArray(d.moves))return;startReplay(d);}).catch(function(){});}function startReplay(rp){stopDemo();state='replay';pendingSubmit=null;seed=rp.seed|0;rngState=seed;sn=[{x:10,y:10},{x:9,y:10},{x:8,y:10}];dr={x:1,y:0};nd=dr;score=0;ms=140;stepN=0;sf();sc.textContent='0';ov.classList.add('hide');var stopBtn=document.getElementById('snk-stop-replay');if(stopBtn)stopBtn.hidden=false;draw();var mIdx=0,rm=rp.moves,DIRS={U:[0,-1],D:[0,1],L:[-1,0],R:[1,0]};function rstep(){if(state!=='replay')return;while(mIdx<rm.length){var m=String(rm[mIdx]),ch=m.charAt(m.length-1),n=parseInt(m,10);if(n>stepN)break;if(n===stepN){var dir=DIRS[ch];if(dir&&(dir[0]!==-dr.x||dir[1]!==-dr.y))nd={x:dir[0],y:dir[1]};}mIdx++;}if(nd.x!==-dr.x||nd.y!==-dr.y)dr=nd;var h={x:sn[0].x+dr.x,y:sn[0].y+dr.y};stepN++;var dead=(h.x<0||h.x>=COLS||h.y<0||h.y>=ROWS)||sn.some(function(s){return s.x===h.x&&s.y===h.y;});if(dead){state='idle';if(stopBtn)stopBtn.hidden=true;mg.innerHTML='<div class="snk-go">Replay done</div><div class="snk-go-sc">'+score+'</div><div class="snk-go-sub">tap canvas to play</div>';ov.classList.remove('hide');return;}sn.unshift(h);if(h.x===fd.x&&h.y===fd.y){score+=10;sc.textContent=score;sf();if(ms>65)ms-=3;}else sn.pop();draw();setTimeout(rstep,ms);}setTimeout(rstep,ms);}function stopReplay(){if(state!=='replay')return;state='idle';var sb=document.getElementById('snk-stop-replay');if(sb)sb.hidden=true;mg.innerHTML='<div class="snk-go">Replay stopped</div><div class="snk-go-sub">tap canvas to play</div>';ov.classList.remove('hide');}var stopBtnEl=document.getElementById('snk-stop-replay');if(stopBtnEl)stopBtnEl.addEventListener('click',stopReplay);var watchHandler=function(ev){var b=ev.target.closest&&ev.target.closest('.snk-watch');if(!b||b.classList.contains('invisible'))return;var g=b.getAttribute('data-g');if(g)fetchAndPlayReplay(g);};if(todayList)todayList.addEventListener('click',watchHandler);if(alltimeList)alltimeList.addEventListener('click',watchHandler);fetchBoth();setTimeout(function(){if(state==='idle'&&!prefersReducedMotion())demoStart();},400);})();</script>`;

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
    this.chronicleSnapshot = null;     // rolling daily stats accumulator; sealed at UTC midnight
    this.chronicleLastPersistAt = 0;   // throttle for snapshot persistence (~once/min)
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
      const csnap = await state.storage.get('chronicleSnapshot');
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
      // Restore in-flight Chronicle snapshot so isolate eviction mid-day
      // doesn't drop the partial accumulator. The seal step in alarm()
      // detects mismatched dates and recovers cleanly either way.
      if (csnap && typeof csnap === 'object') {
        this.chronicleSnapshot = csnap;
        // Migrate older snapshots to the current shape: any field that was
        // added in a later deploy and isn't in the loaded object gets its
        // default value, so the strict-equality null checks in the
        // accumulator work correctly. Without this, new fields would stay
        // undefined and the min/max trackers would silently never update
        // for the rest of the day.
        const fresh = this._chronicleNewSnapshot(csnap.date);
        for (const key in fresh) {
          if (!(key in this.chronicleSnapshot)) {
            this.chronicleSnapshot[key] = fresh[key];
          }
        }
      }
      // Idempotent backfill of skeleton entries for the gap between
      // launch and Chronicle's first sealed day. ~15 storage gets on
      // first run, ~15 fast misses on subsequent runs (existing entries
      // short-circuit). Cost is negligible compared to a missed
      // narrative for those days.
      await this._chronicleBackfill();
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

  // ===== Chronicle =====
  // Auto-generated daily entries archiving the chip's existence. The DO
  // accumulates fields from the chip's stats_update events into a rolling
  // daily snapshot; at UTC midnight the snapshot freezes into a permanent
  // entry under chronicle/YYYY-MM-DD. Templates are plain JS functions in
  // this file, editable without firmware reflash.
  //
  // ===== Fork config =====
  // If you're running this on your own ESP32, edit these values to your
  // own deployment. Day-N counting, the launch entry's date, and the
  // launch prose all hang off these constants.

  _chronicleConfig() {
    return {
      // Day 1 = the day your chip went online. Used by "Day N" markers
      // and the milestone template (day 1, 7, 30, 100, 365, 500, 1000).
      launchYear:  2026,
      launchMonth: 4,   // 1-indexed (April)
      launchDay:   19,

      // The day Chronicle itself started archiving. A locked launch
      // entry gets seeded here on first DO startup; the seal at the
      // following UTC midnight fills in real stats while preserving
      // this body. To revise after first deploy: delete the
      // chronicle/<firstEntryDate> key from DO storage and redeploy.
      firstEntryDate: '2026-05-04',
      firstEntryBody: "May the 4th be with you.\n\nToday the chip starts keeping a daily diary. Every day online becomes a permanent record: visitors, sensor extremes, weather, milestones.\n\nA small, slow archive of one device staying alive.",

      // Day-500 milestone prose. The original deployment references
      // its 2022-2023 ESP32's ~500-day lifespan; forks should change
      // this to something meaningful (or generic) for their context.
      day500Note: "matched the original chip's run",

      // Curator/owner name. Used by self-aware-chip detectors that
      // address the curator by name on long absences. Forks should
      // set this to whoever maintains the device.
      owner_name: 'Tech1k',

      // Owner-defined personal milestones. Calendar-driven anchors that
      // fire once per year on their month-day match. Each entry needs
      // { month, day, clause }. Add more here without code changes
      // (e.g., framing-day, first-guestbook-signing, etc.). April 19
      // intentionally omitted: it's the chip's launch day, so every
      // April 19 from year 1 onward already hits the milestone-template
      // year-anniversary line ("one full year", "two full years", etc.)
      // and a personal anchor would duplicate or fight for the slot.
      ownerMilestones: [
        { month: 6, day: 17, clause: 'The curator turns a year older today.' },
      ],
    };
  }

  _chronicleDayNumber(date) {
    const cfg = this._chronicleConfig();
    const launchUtc = Date.UTC(cfg.launchYear, cfg.launchMonth - 1, cfg.launchDay);
    const [y, m, d] = date.split('-').map(Number);
    const target = Date.UTC(y, m - 1, d);
    return Math.floor((target - launchUtc) / 86400000) + 1;
  }

  // ===== X / Twitter auto-post =====
  // Posts each sealed Chronicle entry to X. Skipped silently when the
  // 4 OAuth 1.0a secrets are missing (X_API_KEY / X_API_SECRET /
  // X_ACCESS_TOKEN / X_ACCESS_SECRET) so deploying without credentials
  // doesn't break the seal path. Each entry is flagged posted_to_x:true
  // after a successful tweet so re-seals + DO restarts don't double-post.

  // RFC 3986 percent-encoding. Stricter than encodeURIComponent: also
  // escapes !*'() per the OAuth 1.0a spec.
  _rfc3986(s) {
    return encodeURIComponent(String(s)).replace(/[!*'()]/g,
      c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
  }

  // Build the OAuth 1.0a Authorization header for an X API request.
  // Body params are NOT included in the signature for v2 JSON endpoints
  // (only URL query params + oauth_* params get signed).
  async _oauth1aHeader(method, url, queryParams) {
    const env = this.env;
    if (!env.X_API_KEY || !env.X_API_SECRET || !env.X_ACCESS_TOKEN || !env.X_ACCESS_SECRET) {
      return null;
    }
    const oauth = {
      oauth_consumer_key: env.X_API_KEY,
      oauth_nonce: crypto.randomUUID().replace(/-/g, ''),
      oauth_signature_method: 'HMAC-SHA1',
      oauth_timestamp: String(Math.floor(Date.now() / 1000)),
      oauth_token: env.X_ACCESS_TOKEN,
      oauth_version: '1.0',
    };
    const all = { ...(queryParams || {}), ...oauth };
    const sorted = Object.keys(all).sort();
    const paramString = sorted
      .map(k => this._rfc3986(k) + '=' + this._rfc3986(all[k]))
      .join('&');
    const baseString = [
      method.toUpperCase(),
      this._rfc3986(url),
      this._rfc3986(paramString),
    ].join('&');
    const signingKey = this._rfc3986(env.X_API_SECRET) + '&' + this._rfc3986(env.X_ACCESS_SECRET);
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
      'raw', enc.encode(signingKey),
      { name: 'HMAC', hash: 'SHA-1' },
      false, ['sign']
    );
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(baseString));
    const sigB64 = btoa(String.fromCharCode(...new Uint8Array(sig)));
    oauth.oauth_signature = sigB64;
    return 'OAuth ' + Object.keys(oauth).sort()
      .map(k => this._rfc3986(k) + '="' + this._rfc3986(oauth[k]) + '"')
      .join(', ');
  }

  // Post a single tweet via X API v2. Returns {ok, id?, status?, error?}.
  // opts.replyTo: post as a reply to this tweet ID (threads under the original).
  async _xPostTweet(text, opts) {
    const url = 'https://api.twitter.com/2/tweets';
    const auth = await this._oauth1aHeader('POST', url, {});
    if (!auth) return { ok: false, error: 'no_credentials' };
    const payload = { text };
    if (opts && opts.replyTo) {
      payload.reply = { in_reply_to_tweet_id: String(opts.replyTo) };
    }
    let resp;
    try {
      resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': auth,
          'Content-Type': 'application/json',
          'User-Agent': 'HelloESP-Chronicle/1.0',
        },
        body: JSON.stringify(payload),
      });
    } catch (e) {
      return { ok: false, error: 'fetch_failed: ' + (e && e.message) };
    }
    if (!resp.ok) {
      const errText = await resp.text().catch(() => '');
      return { ok: false, status: resp.status, error: errText.slice(0, 200) };
    }
    const data = await resp.json().catch(() => null);
    return { ok: true, id: data && data.data && data.data.id };
  }

  // Build tweet text for a Chronicle entry. X's t.co shortener counts
  // every URL as 23 chars regardless of actual length, so we budget for
  // 23 + header + separators and truncate the body to fit. The 25000
  // ceiling matches the X Premium per-tweet character limit; without
  // Premium this would need to drop to 280 (and the truncation logic
  // below kicks in at any cap). Most chronicle bodies sit under 500
  // chars so truncation rarely fires; weekly/monthly summaries can run
  // longer and Premium accommodates them in full.
  _chronicleFormatTweet(entry) {
    let dateLong = entry.date;
    try {
      dateLong = new Date(entry.date + 'T12:00:00Z').toLocaleDateString('en-US', {
        year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC'
      });
    } catch (e) {}
    const header = `Day ${entry.day_number || '?'} · ${dateLong}`;
    const link   = `helloesp.com/chronicle/${entry.date}`;
    const linkBudget = 23; // X t.co shortened length
    const separators = 4;  // two "\n\n" pairs
    const bodyMax = 25000 - header.length - linkBudget - separators;
    let body = entry.body || '';
    if (body.length > bodyMax) {
      // Trim at last whitespace inside budget, then add ellipsis.
      body = body.substring(0, bodyMax - 1).replace(/\s+\S*$/, '') + '…';
    }
    return header + '\n\n' + body + '\n\n' + link;
  }

  // Read entry, check posted flag, post, set flag on success. Idempotent
  // and safe to call from any code path (won't double-post). The in-flight
  // set is claimed synchronously before any await, so two concurrent calls
  // for the same date can't both pass the check.
  async _chronicleMaybePost(date) {
    this.xPostInFlight = this.xPostInFlight || new Set();
    if (this.xPostInFlight.has(date)) return;
    this.xPostInFlight.add(date);
    try {
      const entry = await this.state.storage.get('chronicle/' + date);
      if (!entry) return;
      if (entry.posted_to_x) return;
      const text = this._chronicleFormatTweet(entry);
      const result = await this._xPostTweet(text);
      if (result.ok) {
        // Re-read entry: the X API call yielded, so admin note writes or
        // other handlers may have modified the entry in the meantime. Writing
        // our stale copy back would clobber those changes.
        const fresh = await this.state.storage.get('chronicle/' + date);
        if (!fresh || fresh.posted_to_x) return;
        fresh.posted_to_x = true;
        if (result.id) fresh.x_tweet_id = result.id;
        await this.state.storage.put('chronicle/' + date, fresh);
      } else if (result.error !== 'no_credentials') {
        // Don't log the silent skip path; only real failures.
        console.error('Chronicle X post failed:', result.status, result.error);
      }
    } finally {
      this.xPostInFlight.delete(date);
    }
  }

  // Format an owner-note as a reply tweet. No header (X threads it under
  // the parent), permalink appended so long notes have a "see more" path.
  // 25000 cap matches X Premium; without Premium this would need 280.
  _chronicleFormatNoteReply(entry) {
    const link = `helloesp.com/chronicle/${entry.date}`;
    const linkBudget = 23;
    const separator = 2; // one "\n\n"
    const noteMax = 25000 - linkBudget - separator;
    let note = entry.owner_note || '';
    if (note.length > noteMax) {
      note = note.substring(0, noteMax - 1).replace(/\s+\S*$/, '') + '…';
    }
    return note + '\n\n' + link;
  }

  // Post the owner_note as a reply to the entry's original tweet. Idempotent
  // via note_x_tweet_id; first non-empty note posts, subsequent edits don't
  // (X API v2 has no free tweet-edit). Skipped silently if there's no parent
  // tweet to reply to or no note text. In-flight set is claimed synchronously
  // before any await, so two rapid admin saves can't both pass the flag check
  // and double-post.
  async _chronicleMaybePostNote(date) {
    this.notePostInFlight = this.notePostInFlight || new Set();
    if (this.notePostInFlight.has(date)) return;
    this.notePostInFlight.add(date);
    try {
      const entry = await this.state.storage.get('chronicle/' + date);
      if (!entry) return;
      if (!entry.x_tweet_id) return;       // original tweet never posted; nothing to reply to
      if (!entry.owner_note) return;       // note cleared, or never set
      if (entry.note_x_tweet_id) return;   // already replied
      const text = this._chronicleFormatNoteReply(entry);
      const result = await this._xPostTweet(text, { replyTo: entry.x_tweet_id });
      if (result.ok) {
        // Re-read to avoid clobbering any concurrent modifications during
        // the X API call (e.g. another admin save changing the body).
        const fresh = await this.state.storage.get('chronicle/' + date);
        if (!fresh || fresh.note_x_tweet_id) return;
        if (result.id) fresh.note_x_tweet_id = result.id;
        await this.state.storage.put('chronicle/' + date, fresh);
      } else if (result.error !== 'no_credentials') {
        console.error('Chronicle note reply failed:', result.status, result.error);
      }
    } finally {
      this.notePostInFlight.delete(date);
    }
  }

  // ===== Durability =====
  // DO storage is the live source of truth, but Chronicle is permanent
  // narrative data we don't want to depend on a single backend. Two
  // redundant copies fire after each seal/backfill: (1) immediate write to
  // R2 at state/chronicle/<year>/<date>.json so the entry is durable within
  // seconds, (2) push over WS to the chip which writes to its SD card at
  // /chronicle/<year>/<date>.json (also picked up by the chip's daily SD→R2
  // backup loop, so the SD path piggybacks on existing infrastructure).
  // Year subdir on both surfaces keeps each year's entries grouped so
  // human inspection of the R2 bucket and the chip's SD card stay clean
  // as the archive grows past 1000+ entries.

  // R2 backup. Skipped silently if BACKUP binding isn't configured (forks
  // running without R2 still get the chip-side SD copy).
  async _chronicleBackupToR2(entry) {
    if (!this.env.BACKUP || !entry || !entry.date) return;
    try {
      const year = entry.date.slice(0, 4);
      await this.env.BACKUP.put(
        'state/chronicle/' + year + '/' + entry.date + '.json',
        JSON.stringify(entry, null, 2),
        { httpMetadata: { contentType: 'application/json' } }
      );
    } catch (e) {
      console.error('chronicle R2 backup failed:', e && e.message);
    }
  }

  // Push entry to chip via WS event. The inner entry JSON is encoded as a
  // string-valued field on the event so the chip's indexOf-based parser
  // can extract it without needing a real JSON parser. Skipped silently
  // if the chip is offline; missed entries can be replayed manually or
  // recovered from R2 if needed.
  _chroniclePushToChip(entry) {
    if (!this.espSocket || this.espSocket.readyState !== 1) return;
    if (!entry || !entry.date) return;
    // Reflections (weekly/monthly) live worker-only; chip's SD stores
    // daily entries only. Reflections are aggregations of data already
    // on the chip's SD, plus R2 backup, so a third copy isn't needed.
    if (entry.kind && entry.kind !== 'daily') return;
    try {
      const entryJson = JSON.stringify(entry);
      const msg = JSON.stringify({
        type: 'event',
        event: 'chronicle_seal',
        data: { date: entry.date, entry: entryJson },
      });
      this.espSocket.send(msg);
    } catch (e) {
      console.error('chronicle push to chip failed:', e && e.message);
    }
  }

  // Convenience: fire both backups for an entry. Called after every seal
  // and backfill write so we never have a path that updates DO without
  // also updating R2 + chip-SD. Both helpers no-op silently on failure
  // so the seal flow is never blocked by backup issues.
  async _chronicleBackup(entry) {
    await this._chronicleBackupToR2(entry);
    this._chroniclePushToChip(entry);
  }

  // Full DO storage snapshot to R2. Chronicle entries already get per-seal
  // backups, but other DO state (snake leaderboard + season archives + replays,
  // weather/AQ caches, operational flags) lives only in DO. A daily catch-all
  // snapshot to R2 means DO loss (CF outage, bug-driven mass delete, accidental
  // DO Explorer click) is recoverable from at most yesterday's state. Per-key
  // backups would be more current but more invasive; daily JSON is the lowest-
  // surface-area fix. Output: state/do-snapshot/YYYY-MM-DD.json with all keys.
  async _doSnapshot() {
    if (!this.env.BACKUP) return;
    const today = this._todayUtc();
    const allKeys = {};
    let cursor;
    // Cursor-paginate the DO list since the per-call cap is 1000. With ~100
    // keys today this is one call; future-safe past year-3 chronicle scaling.
    while (true) {
      const opts = { limit: 1000 };
      if (cursor) opts.start = cursor;
      const page = await this.state.storage.list(opts);
      if (page.size === 0) break;
      let lastKey = null;
      for (const [k, v] of page) {
        lastKey = k;
        allKeys[k] = v;
      }
      if (page.size < 1000 || !lastKey) break;
      cursor = lastKey + '\x00';
    }
    const payload = {
      snapshot_at: Math.floor(Date.now() / 1000),
      snapshot_date: today,
      key_count: Object.keys(allKeys).length,
      data: allKeys,
    };
    try {
      await this.env.BACKUP.put('state/do-snapshot/' + today + '.json',
        JSON.stringify(payload),
        { httpMetadata: { contentType: 'application/json' } });
    } catch (e) {
      console.error('do-snapshot R2 write failed:', e && e.message);
      return;
    }
    await this.state.storage.put('lastDoSnapshotDate', today);
    console.log('do-snapshot: wrote', today, 'with', payload.key_count, 'keys');
    // Prune snapshots older than 30 days. R2 storage is cheap but unbounded
    // growth is messy; 30 days is plenty of recovery window.
    try {
      const cutoff = new Date();
      cutoff.setUTCDate(cutoff.getUTCDate() - 30);
      const cutoffStr = cutoff.toISOString().slice(0, 10);
      const list = await this.env.BACKUP.list({ prefix: 'state/do-snapshot/' });
      const toDelete = [];
      for (const obj of (list.objects || [])) {
        const m = obj.key.match(/state\/do-snapshot\/(\d{4}-\d{2}-\d{2})\.json$/);
        if (m && m[1] < cutoffStr) toDelete.push(obj.key);
      }
      if (toDelete.length) {
        await this.env.BACKUP.delete(toDelete);
        console.log('do-snapshot: pruned', toDelete.length, 'old snapshots');
      }
    } catch (e) {
      console.error('do-snapshot prune failed:', e && e.message);
      // Non-fatal: snapshot was written, prune is best-effort.
    }
  }

  // Once-per-UTC-day gate on _doSnapshot. Called from alarm(), short-circuits
  // most of the time after the first call each day. Storage check is cheap
  // (single get), much cheaper than redundantly snapshotting on every alarm.
  async _maybeDoSnapshot() {
    if (!this.env.BACKUP) return;
    const today = this._todayUtc();
    const last = await this.state.storage.get('lastDoSnapshotDate');
    if (last === today) return;
    await this._doSnapshot();
  }

  _chronicleNewSnapshot(date) {
    return {
      date,
      samples: 0,
      // visitors_today is computed as a delta from a lifetime-counter
      // anchor captured at the start of each chip-local day.
      visitors_start: null,
      visitors_today: 0,
      visitors_total: 0,
      countries_total: 0,
      guestbook_approved: 0,
      co2_min: null, co2_max: null,
      voc_min: null, voc_max: null,
      temp_min_f: null, temp_max_f: null,
      humidity_min: null, humidity_max: null,
      pressure_min_hpa: null, pressure_max_hpa: null,
      power_max_w: null,
      energy_start_wh: null,   // anchor: stats.energy_total_wh at first sample of UTC day
      energy_today_wh: 0,      // delta: stats.energy_total_wh - energy_start_wh
      outdoor_latest: null,
      outdoor_temp_min_f: null, outdoor_temp_max_f: null,
      outdoor_aqi_max: null,
      rssi_min: null, rssi_max: null,
      heap_free_min_kb: null,
      reboots: 0,
      prev_uptime_s: null,
      // Hourly buckets indexed 0-23 by chip-local hour. Each metric tracks
      // min and max within the hour. visitors_min/max use the lifetime
      // counter so per-hour visitor count is (max - min). Pre-allocated as
      // null arrays so detectors can distinguish "no data this hour" from
      // 0. Bucket data flows into the sealed entry's stats.hourly blob,
      // unlocking future time-of-day detectors (peak-hour shifts, visitor
      // pattern observations, intraday sensor texture). Only added 2026-05-06
      // so prior entries lack it; consumers must null-check.
      hourly: {
        co2_min:        Array(24).fill(null),
        co2_max:        Array(24).fill(null),
        temp_min_f:     Array(24).fill(null),
        temp_max_f:     Array(24).fill(null),
        visitors_min:   Array(24).fill(null),
        visitors_max:   Array(24).fill(null),
      },
    };
  }

  // Parse the chip's uptime string into seconds. The Uptime library emits
  // the verbose format "X days, Y hours, Z minutes, W seconds"; this also
  // handles compact forms like "5d 3h 12m". Independent regex per unit so
  // the verbose format parses correctly (a single sequential pattern would
  // greedy-match the days portion and skip the rest). Returns null if no
  // unit found. Used for reboot detection via uptime decrement.
  _parseUptimeToSeconds(s) {
    if (typeof s !== 'string') return null;
    const dMatch = s.match(/(\d+)\s*d/);
    const hMatch = s.match(/(\d+)\s*h/);
    const mMatch = s.match(/(\d+)\s*m/);
    const sMatch = s.match(/(\d+)\s*s/);
    const days = dMatch ? parseInt(dMatch[1]) : 0;
    const hours = hMatch ? parseInt(hMatch[1]) : 0;
    const mins = mMatch ? parseInt(mMatch[1]) : 0;
    const secs = sMatch ? parseInt(sMatch[1]) : 0;
    if (!dMatch && !hMatch && !mMatch && !sMatch) return null;
    return days * 86400 + hours * 3600 + mins * 60 + secs;
  }

  async _chronicleAccumulate(stats) {
    if (!stats) return;
    // Prefer the chip-reported chip-local date (driven by config.txt's
    // timezone field on the chip via tzset). This makes /chronicle/<date>
    // align with the chip's lived day rather than UTC. The chronicle is
    // the chip's diary, so a Mountain Time chip's "May 6" entry should
    // cover MT 00:00–23:59, not UTC. Worker stays timezone-blind; the
    // chip is the authority for its own day boundary. Falls back to UTC
    // when the chip hasn't reported (legacy firmware) or NTP isn't synced
    // yet on the chip (today_local arrives empty).
    // Require chip-local time. Without it (chip's NTP and RTC both
    // failed, or pre-1.4 firmware that didn't emit the field) we don't
    // know the chip's day boundary. Defaulting to UTC could create a
    // wrong-date snap when chip is in a UTC-offset timezone, which
    // later corrects via mismatch + premature seal once chip-local time
    // recovers. Skipping the sample is safer: snap waits, the next event
    // with valid today_local picks up cleanly. Trade-off accepted: if
    // chip's clock is permanently broken, no chronicle accumulation.
    const reported = stats.today_local;
    const today = (typeof reported === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(reported))
      ? reported
      : null;
    if (!today) return;
    // Refuse backward time travel. If incoming event's chip-local date is
    // before the current snap.date, treat as stale (delayed, reordered,
    // or spoofed) and skip. WebSocket guarantees in-order delivery within
    // a session and reconnects don't replay messages, so this should
    // never fire in normal operation; the warning log surfaces any real
    // occurrence as evidence of a bug or architectural change.
    if (this.chronicleSnapshot && today < this.chronicleSnapshot.date) {
      console.warn('chronicle: ignoring stale stats event, today=' + today
        + ' < snap.date=' + this.chronicleSnapshot.date);
      return;
    }
    if (this.chronicleSnapshot && this.chronicleSnapshot.date !== today) {
      // Day rolled over since the last sample. Seal yesterday's snapshot
      // BEFORE we replace it, so the alarm's 30s tick can't lose those
      // last samples (a stats_update can arrive within seconds of the
      // chip's local midnight, well before the alarm fires).
      await this._chronicleMaybeSeal();
    }
    if (!this.chronicleSnapshot || this.chronicleSnapshot.date !== today) {
      this.chronicleSnapshot = this._chronicleNewSnapshot(today);
    }
    const s = this.chronicleSnapshot;
    s.samples++;
    // visitors_today via lifetime-counter delta. Anchor captured on first
    // sample after snap rotation; subsequent samples compute (current -
    // anchor) as today's count.
    if (typeof stats.visitors === 'number' && stats.visitors >= 0) {
      if (s.visitors_start == null) {
        if (typeof stats.daily_visitors === 'number' && stats.daily_visitors >= 0) {
          s.visitors_start = stats.visitors - stats.daily_visitors;
          s.visitors_today = stats.daily_visitors;
        } else {
          s.visitors_start = stats.visitors;
          s.visitors_today = 0;
        }
      }
      s.visitors_total = Math.max(s.visitors_total, stats.visitors);
      s.visitors_today = Math.max(s.visitors_today, stats.visitors - s.visitors_start);
    }
    if (typeof stats.countries === 'number') {
      s.countries_total = Math.max(s.countries_total, stats.countries);
    }
    if (typeof stats.guestbook_approved === 'number') {
      s.guestbook_approved = Math.max(s.guestbook_approved, stats.guestbook_approved);
    }
    if (typeof stats.co2_ppm === 'number' && stats.co2_ppm > 0) {
      if (s.co2_min === null || stats.co2_ppm < s.co2_min) s.co2_min = stats.co2_ppm;
      if (s.co2_max === null || stats.co2_ppm > s.co2_max) s.co2_max = stats.co2_ppm;
    }
    if (typeof stats.voc_ppb === 'number' && stats.voc_ppb > 0) {
      if (s.voc_min === null || stats.voc_ppb < s.voc_min) s.voc_min = stats.voc_ppb;
      if (s.voc_max === null || stats.voc_ppb > s.voc_max) s.voc_max = stats.voc_ppb;
    }
    const tf = stats.temperature && stats.temperature.fahrenheit;
    if (typeof tf === 'number' && isFinite(tf)) {
      if (s.temp_min_f === null || tf < s.temp_min_f) s.temp_min_f = tf;
      if (s.temp_max_f === null || tf > s.temp_max_f) s.temp_max_f = tf;
    }
    if (typeof stats.humidity_percent === 'number') {
      if (s.humidity_min === null || stats.humidity_percent < s.humidity_min) s.humidity_min = stats.humidity_percent;
      if (s.humidity_max === null || stats.humidity_percent > s.humidity_max) s.humidity_max = stats.humidity_percent;
    }
    if (typeof stats.pressure_hpa === 'number' && isFinite(stats.pressure_hpa) && stats.pressure_hpa > 0) {
      if (s.pressure_min_hpa === null || stats.pressure_hpa < s.pressure_min_hpa) s.pressure_min_hpa = stats.pressure_hpa;
      if (s.pressure_max_hpa === null || stats.pressure_hpa > s.pressure_max_hpa) s.pressure_max_hpa = stats.pressure_hpa;
    }
    if (typeof stats.power_w === 'number' && isFinite(stats.power_w)) {
      if (s.power_max_w === null || stats.power_w > s.power_max_w) s.power_max_w = stats.power_w;
    }
    // Energy via lifetime delta (energy_total_wh). Same reasoning as
    // visitors: energy_today_wh on the chip is daily and resets at chip-
    // local midnight, so a high-water mark over the UTC day captures
    // chip-local-day-end values and double-counts at the timezone
    // boundary. energy_total_wh is the lifetime cumulative kWh from
    // Shelly; only ever grows. Skipped silently when no Shelly is
    // configured (energy_total_wh field absent from stats). Same
    // high-water-mark protection as visitors: a counter decrement
    // (Shelly reset; rare) freezes today's energy at pre-decrement peak
    // rather than dropping to zero.
    if (typeof stats.energy_total_wh === 'number' && isFinite(stats.energy_total_wh)
        && stats.energy_total_wh >= 0) {
      if (s.energy_start_wh == null) s.energy_start_wh = stats.energy_total_wh;
      s.energy_today_wh = Math.max(s.energy_today_wh, stats.energy_total_wh - s.energy_start_wh);
    }
    // WiFi signal strength range (dBm; closer to 0 is stronger).
    if (typeof stats.rssi === 'number' && isFinite(stats.rssi) && stats.rssi < 0) {
      if (s.rssi_min === null || stats.rssi < s.rssi_min) s.rssi_min = stats.rssi;
      if (s.rssi_max === null || stats.rssi > s.rssi_max) s.rssi_max = stats.rssi;
    }
    // Min free heap during the day. Derive from used_bytes + used_percent
    // (chip publishes both; total = used_bytes / used_percent * 100).
    if (stats.memory && typeof stats.memory.used_bytes === 'number'
        && typeof stats.memory.used_percent === 'number'
        && stats.memory.used_percent > 0 && stats.memory.used_percent < 100) {
      const totalBytes = stats.memory.used_bytes / (stats.memory.used_percent / 100);
      const freeKb = (totalBytes - stats.memory.used_bytes) / 1024;
      if (isFinite(freeKb) && freeKb >= 0) {
        if (s.heap_free_min_kb === null || freeKb < s.heap_free_min_kb) s.heap_free_min_kb = freeKb;
      }
    }
    // Reboot detection via uptime decrement. If this sample's uptime is
    // smaller than the prior one we saw, the chip restarted between samples.
    const uptimeS = this._parseUptimeToSeconds(stats.uptime);
    if (uptimeS !== null) {
      if (s.prev_uptime_s !== null && uptimeS < s.prev_uptime_s) {
        s.reboots = (s.reboots || 0) + 1;
      }
      s.prev_uptime_s = uptimeS;
    }
    if (stats.outdoor && typeof stats.outdoor === 'object') {
      // Snapshot the most recent outdoor observation rather than averaging.
      // Templates only need a representative number per day, and weather
      // intra-day variation is its own narrative beat we can mine later.
      s.outdoor_latest = {
        temp_f:        stats.outdoor.temp_f,
        humidity:      stats.outdoor.humidity,
        weather_code:  stats.outdoor.weather_code,
        us_aqi:        stats.outdoor.us_aqi,
        pressure_hpa:  stats.outdoor.pressure_hpa,
      };
      // Per-day outdoor temp range and AQI peak. The "latest" snapshot above
      // gives templates a representative point; these give the stats card
      // the day's actual envelope.
      if (typeof stats.outdoor.temp_f === 'number' && isFinite(stats.outdoor.temp_f)) {
        if (s.outdoor_temp_min_f === null || stats.outdoor.temp_f < s.outdoor_temp_min_f) s.outdoor_temp_min_f = stats.outdoor.temp_f;
        if (s.outdoor_temp_max_f === null || stats.outdoor.temp_f > s.outdoor_temp_max_f) s.outdoor_temp_max_f = stats.outdoor.temp_f;
      }
      if (typeof stats.outdoor.us_aqi === 'number' && isFinite(stats.outdoor.us_aqi) && stats.outdoor.us_aqi >= 0) {
        if (s.outdoor_aqi_max === null || stats.outdoor.us_aqi > s.outdoor_aqi_max) s.outdoor_aqi_max = stats.outdoor.us_aqi;
      }
    }

    // Hourly bucket update. Uses chip-local hour from stats.today_local_hour
    // (emitted by chip via the same getLocalTime call that populates
    // today_local) so buckets align with the chip's lived day. Validates
    // the hour is in 0-23 range; if missing/invalid, the sample contributes
    // to daily aggregates above but skips bucket update. Detectors can
    // null-check each bucket slot to distinguish "no data this hour" from
    // a real reading.
    if (s.hourly && typeof stats.today_local_hour === 'number'
        && stats.today_local_hour >= 0 && stats.today_local_hour < 24) {
      const hr = stats.today_local_hour | 0;
      const updateMin = (arr, val) => {
        if (typeof val !== 'number' || !isFinite(val)) return;
        if (arr[hr] === null || val < arr[hr]) arr[hr] = val;
      };
      const updateMax = (arr, val) => {
        if (typeof val !== 'number' || !isFinite(val)) return;
        if (arr[hr] === null || val > arr[hr]) arr[hr] = val;
      };
      if (typeof stats.co2_ppm === 'number' && stats.co2_ppm > 0) {
        updateMin(s.hourly.co2_min, stats.co2_ppm);
        updateMax(s.hourly.co2_max, stats.co2_ppm);
      }
      if (stats.temperature && typeof stats.temperature.fahrenheit === 'number') {
        updateMin(s.hourly.temp_min_f, stats.temperature.fahrenheit);
        updateMax(s.hourly.temp_max_f, stats.temperature.fahrenheit);
      }
      if (typeof stats.visitors === 'number' && stats.visitors >= 0) {
        updateMin(s.hourly.visitors_min, stats.visitors);
        updateMax(s.hourly.visitors_max, stats.visitors);
      }
    }
  }

  // Seed the launch entry on first DO startup. Earlier days the chip was
  // alive but Chronicle wasn't watching; rather than retro-narrate routine
  // days, the index honestly opens on the day archiving started. The
  // entry is locked: at UTC midnight the seal preserves this body but
  // fills in real stats from the day's accumulator. Idempotent: the entry
  // write is skipped if it already exists, but the X post is always fired
  // (gated by the posted_to_x flag) so a redeploy after creds-were-missing
  // can still publish the launch tweet. To revise the prose, delete the
  // chronicle/<firstEntryDate> key from DO storage and redeploy.
  async _chronicleBackfill() {
    const cfg = this._chronicleConfig();
    const launchDate = cfg.firstEntryDate;
    const existing = await this.state.storage.get('chronicle/' + launchDate);
    if (!existing) {
      await this.state.storage.put('chronicle/' + launchDate, {
        date: launchDate,
        day_number: this._chronicleDayNumber(launchDate),
        template_id: 'launch',
        body: cfg.firstEntryBody,
        stats: {},
        sealed_at: Math.floor(Date.now() / 1000),
        locked: true,
      });
    }
    // Always fire the post; _chronicleMaybePost is idempotent via the
    // posted_to_x flag, so this is safe whether the entry was just written
    // or already existed from a prior deploy that ran before X creds existed.
    this._chronicleMaybePost(launchDate).catch(e =>
      console.error('launch tweet failed:', e && e.message));
    // Backup to R2 + push to chip on every backfill run, not just first
    // write. Idempotent (R2 just overwrites the same key, chip overwrites
    // the SD file), and means a redeploy after the chip was offline at
    // launch time still gets the entry onto the SD card.
    const fresh = await this.state.storage.get('chronicle/' + launchDate);
    if (fresh) {
      this._chronicleBackup(fresh).catch(e =>
        console.error('chronicle backup failed:', e && e.message));
    }
  }

  async _chronicleMaybeSeal() {
    // Wrapped in blockConcurrencyWhile so the entry RMW (get existing,
    // build merged entry, put) can't interleave with chronicle_note_set's
    // own RMW. Without this, an admin note arriving mid-seal could either
    // lose its write or clobber the seal's writes. fire-and-forget post +
    // backup live OUTSIDE the lock so they don't extend it through the
    // ~500 ms X API call.
    const snap = this.chronicleSnapshot;
    if (!snap) return;
    // Require valid chip-local time. Without it (e.g., alarm fired right
    // after isolate restart before any chip stats arrived) we don't know
    // the chip's day boundary, and defaulting to UTC can prematurely seal
    // a still-in-progress chip-local day when chip is in a UTC-offset
    // timezone. Postpone: the next chip stats event triggers seal through
    // the accumulator path with today_local guaranteed in scope. Trade-off:
    // if chip's NTP never works, snap accumulates indefinitely; acceptable
    // because the alternative (UTC-guess seals) corrupts entries.
    let today = null;
    if (this.lastStats) {
      try {
        const tl = JSON.parse(this.lastStats).today_local;
        if (typeof tl === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(tl)) today = tl;
      } catch (e) {}
    }
    if (!today) return;
    // Helper: create a new snap and capture chip's lifetime visitors
    // count from lastStats as the new day's anchor. Without this, the
    // rotated snap relies on the accumulator's back-compute (anchor =
    // lifetime - daily) on the next stats event, which mishandles stale
    // chip-side daily counters that haven't reset across midnight.
    // Capturing the anchor at rotation, from the always-monotonic
    // lifetime counter, sidesteps any chip-side daily timing issues.
    const rotateSnap = (toDate) => {
      const fresh = this._chronicleNewSnapshot(toDate);
      if (this.lastStats) {
        try {
          const v = JSON.parse(this.lastStats).visitors;
          if (typeof v === 'number' && v >= 0) {
            fresh.visitors_start = v;
            fresh.visitors_today = 0;
          }
        } catch (e) {}
      }
      return fresh;
    };
    let sealedEntry = null;
    let didSeal = false;
    let newToday = null;
    await this.state.blockConcurrencyWhile(async () => {
      if (snap.date === today) return; // still the same chip-local day
      if (snap.samples === 0) {
        // No stats events that day (chip offline all day or DO just spun
        // up). Skip the entry; an empty Chronicle row would lie about the
        // chip having been awake to notice anything. Still set newToday
        // so reflection seals fire downstream: a chip-offline day shouldn't
        // suppress the week's reflection if prior days had data.
        this.chronicleSnapshot = rotateSnap(today);
        await this.state.storage.put('chronicleSnapshot', this.chronicleSnapshot);
        newToday = today;
        return;
      }
      const dayNum = this._chronicleDayNumber(snap.date);
      const ctx = { dayNumber: dayNum };
      const existing = await this.state.storage.get('chronicle/' + snap.date);
      // Fully immutable entries: locked + stats_locked. Set on entries
      // that were manually corrected after a bug surfaced wrong data.
      // The seal short-circuits here, rotating the snapshot without
      // writing a new entry. Without this guard, the night's seal would
      // overwrite the corrected stats with whatever the snap accumulated.
      // Distinct from plain `locked` (used by the launch seed): locked
      // alone preserves body+template_id but lets stats refresh on each
      // seal. stats_locked makes the entry fully sealed-in-stone.
      if (existing && existing.locked && existing.stats_locked) {
        this.chronicleSnapshot = rotateSnap(today);
        await this.state.storage.put('chronicleSnapshot', this.chronicleSnapshot);
        newToday = today;
        return;
      }
      const { template_id, body } = await this._chronicleRender(snap, ctx);
      // Locked entries (the launch seed, future hand-curated anchors)
      // keep their owner-authored body and template_id when the day's
      // seal fires. Stats still get filled in from the day's accumulator,
      // so the entry reads as "owner narrative + chip's real numbers."
      const isLocked = existing && existing.locked;
      const entry = {
        date: snap.date,
        day_number: dayNum,
        template_id: isLocked ? existing.template_id : template_id,
        body:        isLocked ? existing.body        : body,
        stats: this._chronicleStatsSummary(snap),
        sealed_at:   isLocked ? existing.sealed_at   : Math.floor(Date.now() / 1000),
        // Preserve owner_note across re-seals (shouldn't happen normally,
        // but a same-day reseal during clock skew shouldn't drop curatorial
        // work). With blockConcurrencyWhile this is now belt-and-suspenders
        // since the note handler can't write between our get and put.
        ...(existing && existing.owner_note ? { owner_note: existing.owner_note } : {}),
        // Preserve X-post idempotency flags so a reseal doesn't double-post.
        ...(existing && existing.posted_to_x ? { posted_to_x: existing.posted_to_x } : {}),
        ...(existing && existing.x_tweet_id ? { x_tweet_id: existing.x_tweet_id } : {}),
        ...(existing && existing.note_x_tweet_id ? { note_x_tweet_id: existing.note_x_tweet_id } : {}),
        ...(isLocked ? { locked: true } : {}),
      };
      await this.state.storage.put('chronicle/' + snap.date, entry);
      this.chronicleSnapshot = rotateSnap(today);
      await this.state.storage.put('chronicleSnapshot', this.chronicleSnapshot);
      sealedEntry = entry;
      didSeal = true;
      newToday = today;
    });
    if (didSeal && sealedEntry) {
      this._chronicleMaybePost(sealedEntry.date).catch(e =>
        console.error('chronicle tweet failed:', e && e.message));
      this._chronicleBackup(sealedEntry).catch(e =>
        console.error('chronicle backup failed:', e && e.message));
    }
    // Fire-and-forget: if today is Monday or 1st, seal previous week or
    // month as a reflection. Reflections write to their own keys so they
    // don't conflict with daily seal state and live outside the lock.
    // Triggered on any chip-local-day rollover (newToday set), independent
    // of whether yesterday's daily entry got written: a chip-offline day
    // or a stats_locked entry shouldn't suppress the week's reflection,
    // since the reflection aggregates from already-sealed prior days.
    if (newToday) {
      this._chronicleMaybeSealReflections(newToday).catch(e =>
        console.error('reflection seal failed:', e && e.message));
    }
  }

  _chronicleStatsSummary(snap) {
    return {
      visitors:           snap.visitors_today,
      visitors_total:     snap.visitors_total,
      countries:          snap.countries_total,
      guestbook_approved: snap.guestbook_approved,
      co2_min:            snap.co2_min,
      co2_max:            snap.co2_max,
      voc_min:            snap.voc_min,
      voc_max:            snap.voc_max,
      temp_min_f:         snap.temp_min_f,
      temp_max_f:         snap.temp_max_f,
      humidity_min:       snap.humidity_min,
      humidity_max:       snap.humidity_max,
      pressure_min_hpa:   snap.pressure_min_hpa,
      pressure_max_hpa:   snap.pressure_max_hpa,
      power_max_w:        snap.power_max_w,
      energy_today_wh:    snap.energy_today_wh,
      outdoor:            snap.outdoor_latest,
      outdoor_temp_min_f: snap.outdoor_temp_min_f,
      outdoor_temp_max_f: snap.outdoor_temp_max_f,
      outdoor_aqi_max:    snap.outdoor_aqi_max,
      rssi_min:           snap.rssi_min,
      rssi_max:           snap.rssi_max,
      heap_free_min_kb:   snap.heap_free_min_kb,
      reboots:            snap.reboots,
      samples:            snap.samples,
      hourly:             snap.hourly || null,
    };
  }

  // Compute the unix timestamp of the next chip-local midnight (the
  // moment of the next chronicle seal). Derived from the chip's most
  // recent today_local_hour reading: chip-local minutes/seconds match
  // UTC's (whole-hour TZ assumption holds for all standard zones), so
  // chip-local seconds-into-day = today_local_hour*3600 + UTC mins+secs.
  // Returns null if no recent chip-local hour available; clients fall
  // back to UTC midnight in that case. Used by chronicle.html countdowns
  // so the displayed "next entry in HH:MM:SS" matches the actual seal
  // time rather than the wrong-by-TZ-offset UTC midnight.
  _nextSealUnix() {
    if (!this.lastStats) return null;
    let hour = null;
    try {
      const v = JSON.parse(this.lastStats).today_local_hour;
      if (typeof v === 'number' && v >= 0 && v < 24) hour = v;
    } catch (e) { return null; }
    if (hour === null) return null;
    const now = new Date();
    const chipLocalSecondsToday = hour * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds();
    const secondsToMidnight = 86400 - chipLocalSecondsToday;
    return Math.floor(now.getTime() / 1000) + secondsToMidnight;
  }

  // ===== Chronicle reflections (weekly + monthly) =====
  // Auto-generated reflective entries that aggregate the prior week or
  // month of daily Chronicle data into a "the chip looks back" narrative.
  // Storage parallel to daily entries: chronicle/YYYY-WNN, chronicle/YYYY-MM.
  // Triggered after the daily seal: when chip-local today is a Monday, the
  // just-completed week (Mon-Sun ending yesterday) gets sealed; when it's
  // the 1st, the just-completed month gets sealed. Reflections are
  // worker-only (not pushed to chip's SD); chip has the daily entries on
  // its own SD plus R2 backup, so reflections being aggregations of that
  // data don't need a third copy.

  _isoWeekOf(date) {
    // Returns { year, week } for ISO week containing `date` (a Date object).
    const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    const weekNum = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    return { year: d.getUTCFullYear(), week: weekNum };
  }

  _dateStringToDate(s) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s || '');
    if (!m) return null;
    return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  }

  _fmtDateString(d) {
    return d.getUTCFullYear() + '-' +
      String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
      String(d.getUTCDate()).padStart(2, '0');
  }

  // Walk a date range (inclusive both ends) in chip-local-date space,
  // pull each daily chronicle entry from DO storage, and fold into one
  // aggregate. Returns null when no daily entries exist in the range
  // (caller skips sealing on empty periods).
  async _chronicleAggregateRange(startDate, endDate) {
    const start = this._dateStringToDate(startDate);
    const end   = this._dateStringToDate(endDate);
    if (!start || !end) return null;
    const agg = {
      days_with_data: 0,
      visitors_total: 0,
      countries_max: 0,
      co2_min: null, co2_max: null,
      voc_min: null, voc_max: null,
      temp_min_f: null, temp_max_f: null,
      humidity_min: null, humidity_max: null,
      pressure_min_hpa: null, pressure_max_hpa: null,
      power_max_w: null,
      energy_total_wh: 0,
      outdoor_temp_min_f: null, outdoor_temp_max_f: null,
      outdoor_aqi_max: null,
      reboots_total: 0,
      notes_count: 0,
      // Notable-day pointers: which date inside the period hit each
      // extreme. Used by weekly/monthly templates to surface "best day was
      // Tuesday" style narrative texture instead of pure aggregate stats.
      busiest_day: null,    // {date, visitors}
      quietest_day: null,   // {date, visitors}
      hottest_day: null,    // {date, temp_max_f}
      coldest_day: null,    // {date, temp_min_f}
      highest_co2_day: null,
      reboot_dates: [],
      note_dates: [],
      // Notable daily-template hits within the period: drives weekly /
      // monthly variant selection. milestone_days preserves day_number
      // so the template can name the milestone ("Day 30 fell on Wed").
      milestone_days: [],   // [{date, day_number}]
      record_days: [],      // [{date, template_id}]
      max_co2_under_streak: 0,
      max_co2_over_streak: 0,
    };
    let curUnderStreak = 0, curOverStreak = 0;
    const upd = (key, val, mode) => {
      if (val == null || !isFinite(val)) return;
      if (mode === 'min') agg[key] = (agg[key] === null) ? val : Math.min(agg[key], val);
      else if (mode === 'max') agg[key] = (agg[key] === null) ? val : Math.max(agg[key], val);
    };
    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
      const ds = this._fmtDateString(d);
      const entry = await this.state.storage.get('chronicle/' + ds);
      if (!entry || entry.kind && entry.kind !== 'daily') {
        // Missing day or wrong kind breaks the streak counters too.
        curUnderStreak = 0; curOverStreak = 0;
        continue;
      }
      const s = entry.stats || {};
      agg.days_with_data++;
      if (typeof s.visitors === 'number') {
        agg.visitors_total += s.visitors;
        if (!agg.busiest_day || s.visitors > agg.busiest_day.visitors) {
          agg.busiest_day = { date: ds, visitors: s.visitors };
        }
        if (!agg.quietest_day || s.visitors < agg.quietest_day.visitors) {
          agg.quietest_day = { date: ds, visitors: s.visitors };
        }
      }
      if (typeof s.countries === 'number' && s.countries > agg.countries_max) agg.countries_max = s.countries;
      upd('co2_min', s.co2_min, 'min'); upd('co2_max', s.co2_max, 'max');
      if (typeof s.co2_max === 'number' && (!agg.highest_co2_day || s.co2_max > agg.highest_co2_day.co2_max)) {
        agg.highest_co2_day = { date: ds, co2_max: s.co2_max };
      }
      // Within-period CO₂ streak tracking. Resets on missing data or when
      // a day's value crosses out of the streak threshold. Tracks the
      // longest run seen in the period for weekly_streak template gating.
      if (typeof s.co2_max === 'number') {
        if (s.co2_max < 800) {
          curUnderStreak++;
          if (curUnderStreak > agg.max_co2_under_streak) agg.max_co2_under_streak = curUnderStreak;
          curOverStreak = 0;
        } else if (s.co2_max >= 1000) {
          curOverStreak++;
          if (curOverStreak > agg.max_co2_over_streak) agg.max_co2_over_streak = curOverStreak;
          curUnderStreak = 0;
        } else {
          curUnderStreak = 0; curOverStreak = 0;
        }
      } else {
        curUnderStreak = 0; curOverStreak = 0;
      }
      upd('voc_min', s.voc_min, 'min'); upd('voc_max', s.voc_max, 'max');
      upd('temp_min_f', s.temp_min_f, 'min'); upd('temp_max_f', s.temp_max_f, 'max');
      if (typeof s.temp_max_f === 'number' && (!agg.hottest_day || s.temp_max_f > agg.hottest_day.temp_max_f)) {
        agg.hottest_day = { date: ds, temp_max_f: s.temp_max_f };
      }
      if (typeof s.temp_min_f === 'number' && (!agg.coldest_day || s.temp_min_f < agg.coldest_day.temp_min_f)) {
        agg.coldest_day = { date: ds, temp_min_f: s.temp_min_f };
      }
      upd('humidity_min', s.humidity_min, 'min'); upd('humidity_max', s.humidity_max, 'max');
      upd('pressure_min_hpa', s.pressure_min_hpa, 'min'); upd('pressure_max_hpa', s.pressure_max_hpa, 'max');
      upd('power_max_w', s.power_max_w, 'max');
      if (typeof s.energy_today_wh === 'number' && s.energy_today_wh >= 0) agg.energy_total_wh += s.energy_today_wh;
      upd('outdoor_temp_min_f', s.outdoor_temp_min_f, 'min'); upd('outdoor_temp_max_f', s.outdoor_temp_max_f, 'max');
      upd('outdoor_aqi_max', s.outdoor_aqi_max, 'max');
      if (typeof s.reboots === 'number' && s.reboots > 0) {
        agg.reboots_total += s.reboots;
        agg.reboot_dates.push(ds);
      }
      if (entry.owner_note) {
        agg.notes_count++;
        agg.note_dates.push(ds);
      }
      // Notable daily templates that drove this entry. Used by weekly /
      // monthly variant dispatch to pick a milestone- or record-flavored
      // template instead of the plain summary.
      if (entry.template_id === 'milestone' && typeof entry.day_number === 'number') {
        agg.milestone_days.push({ date: ds, day_number: entry.day_number });
      }
      if (entry.template_id === 'record') {
        agg.record_days.push({ date: ds });
      }
    }
    return agg;
  }

  _fmtPeriodLong(startDate, endDate) {
    // "May 4 to May 10, 2026" or "May 4 to June 1, 2026" depending on span.
    const s = this._dateStringToDate(startDate);
    const e = this._dateStringToDate(endDate);
    if (!s || !e) return startDate + ' to ' + endDate;
    const opts = { month: 'long', day: 'numeric', timeZone: 'UTC' };
    const sStr = s.toLocaleDateString('en-US', opts);
    const eStr = e.toLocaleDateString('en-US', opts);
    return sStr + ' to ' + eStr + ', ' + e.getUTCFullYear();
  }

  _fmtDayOfWeek(dateStr) {
    const d = this._dateStringToDate(dateStr);
    if (!d) return dateStr;
    return d.toLocaleDateString('en-US', { weekday: 'long', timeZone: 'UTC' });
  }

  _fmtMonthDay(dateStr) {
    const d = this._dateStringToDate(dateStr);
    if (!d) return dateStr;
    return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', timeZone: 'UTC' });
  }

  _chronicleWeeklyTemplate(weekKey, periodStart, periodEnd, agg, skipHeader) {
    const m = /^(\d{4})-W(\d{2})$/.exec(weekKey);
    const weekN = m ? parseInt(m[2], 10) : 0;
    const yr = m ? m[1] : '';
    const parts = [];
    if (!skipHeader) {
      parts.push(`Week ${weekN} of ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}.`);
    }
    if (agg.days_with_data === 7) {
      parts.push(`Seven days of records.`);
    } else if (agg.days_with_data > 0) {
      parts.push(`${agg.days_with_data} day${agg.days_with_data === 1 ? '' : 's'} of records.`);
    }
    if (agg.visitors_total > 0) {
      parts.push(`${agg.visitors_total} visit${agg.visitors_total === 1 ? '' : 's'}, ${agg.countries_max} countries on the map by week's end.`);
    }
    // Notable visitor days: surface the busiest and quietest weekdays
    // when they're meaningfully different. Skip when busy/quiet are the
    // same day (single-day weeks) or when totals are too small to vary.
    if (agg.busiest_day && agg.quietest_day && agg.busiest_day.date !== agg.quietest_day.date
        && agg.busiest_day.visitors > 0
        && (agg.busiest_day.visitors - agg.quietest_day.visitors) >= 50) {
      parts.push(`${this._fmtDayOfWeek(agg.busiest_day.date)} was busiest with ${agg.busiest_day.visitors} visits; ${this._fmtDayOfWeek(agg.quietest_day.date)} was quietest with ${agg.quietest_day.visitors}.`);
    }
    if (agg.co2_max !== null) {
      parts.push(`Indoor CO₂ ${agg.co2_min} to ${agg.co2_max} ppm.`);
      if (agg.highest_co2_day && agg.days_with_data > 1) {
        parts.push(`Peak landed on ${this._fmtDayOfWeek(agg.highest_co2_day.date)}.`);
      }
    }
    if (agg.temp_max_f !== null) {
      parts.push(`Indoor ${Math.round(agg.temp_min_f)} to ${Math.round(agg.temp_max_f)}°F.`);
    }
    if (agg.energy_total_wh > 0) {
      const wh = agg.energy_total_wh;
      const display = wh >= 1000 ? `${(wh / 1000).toFixed(2)} kWh` : `${Math.round(wh)} Wh`;
      parts.push(`${display} drawn through the week.`);
    }
    if (agg.reboots_total > 0) {
      const dows = agg.reboot_dates.map(d => this._fmtDayOfWeek(d));
      parts.push(`${agg.reboots_total} reboot${agg.reboots_total === 1 ? '' : 's'}${dows.length ? ' (' + dows.join(', ') + ')' : ''}.`);
    } else {
      parts.push(`Stayed up the whole way.`);
    }
    if (agg.notes_count > 0) {
      parts.push(`${agg.notes_count} curated note${agg.notes_count === 1 ? '' : 's'} from the week.`);
    }
    return parts.join(' ');
  }

  _chronicleMonthlyTemplate(monthKey, agg, skipHeader) {
    const m = /^(\d{4})-(\d{2})$/.exec(monthKey);
    if (!m) return '';
    const year = m[1];
    const monthName = new Date(Date.UTC(+m[1], +m[2] - 1, 1)).toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
    const parts = [];
    if (!skipHeader) {
      parts.push(`${monthName} ${year} in review.`);
    }
    if (agg.days_with_data > 0) {
      parts.push(`${agg.days_with_data} day${agg.days_with_data === 1 ? '' : 's'} of recorded chip experience.`);
    }
    if (agg.visitors_total > 0) {
      parts.push(`${agg.visitors_total} total visit${agg.visitors_total === 1 ? '' : 's'}, ${agg.countries_max} countries reached.`);
    }
    // Surface the month's notable visitor days as specific dates.
    if (agg.busiest_day && agg.quietest_day && agg.busiest_day.date !== agg.quietest_day.date
        && agg.busiest_day.visitors > 0
        && (agg.busiest_day.visitors - agg.quietest_day.visitors) >= 100) {
      parts.push(`Busiest day was ${this._fmtMonthDay(agg.busiest_day.date)} with ${agg.busiest_day.visitors} visits; quietest was ${this._fmtMonthDay(agg.quietest_day.date)} with ${agg.quietest_day.visitors}.`);
    }
    if (agg.co2_max !== null) {
      const co2Range = `Indoor CO₂ ranged ${agg.co2_min} to ${agg.co2_max} ppm across the month.`;
      if (agg.highest_co2_day && agg.days_with_data > 3) {
        parts.push(co2Range + ` Peak fell on ${this._fmtMonthDay(agg.highest_co2_day.date)}.`);
      } else {
        parts.push(co2Range);
      }
    }
    if (agg.temp_max_f !== null) {
      parts.push(`Indoor temperature ran ${Math.round(agg.temp_min_f)} to ${Math.round(agg.temp_max_f)}°F.`);
    }
    if (agg.outdoor_temp_max_f !== null) {
      const outsideRange = `Outside ${Math.round(agg.outdoor_temp_min_f)} to ${Math.round(agg.outdoor_temp_max_f)}°F.`;
      if (agg.hottest_day && agg.coldest_day && agg.days_with_data > 3
          && agg.hottest_day.date !== agg.coldest_day.date) {
        parts.push(outsideRange + ` Hottest on ${this._fmtMonthDay(agg.hottest_day.date)}, coldest on ${this._fmtMonthDay(agg.coldest_day.date)}.`);
      } else {
        parts.push(outsideRange);
      }
    }
    if (agg.energy_total_wh > 0) {
      const wh = agg.energy_total_wh;
      const display = wh >= 1000 ? `${(wh / 1000).toFixed(2)} kWh` : `${Math.round(wh)} Wh`;
      parts.push(`${display} of electricity drawn.`);
    }
    if (agg.reboots_total > 0) {
      parts.push(`${agg.reboots_total} reboot${agg.reboots_total === 1 ? '' : 's'} across the month.`);
    } else {
      parts.push(`Zero reboots; uptime held all month.`);
    }
    if (agg.notes_count > 0) {
      parts.push(`${agg.notes_count} curated note${agg.notes_count === 1 ? '' : 's'}.`);
    }
    return parts.join(' ');
  }

  // ===== Weekly + monthly template variants =====
  // Same shape as daily template selection: more-specific patterns first,
  // summary as the fallback. Each variant has a "headline" framing that
  // leads the body, then the rest of the aggregate stats follow via the
  // existing summary template body. The kind pill conveys daily vs weekly
  // vs monthly; template_id pill conveys which variety of summary fired.

  _chronicleWeeklyTemplateMilestone(weekKey, periodStart, periodEnd, agg) {
    if (!agg.milestone_days || agg.milestone_days.length === 0) return null;
    const m = /^(\d{4})-W(\d{2})$/.exec(weekKey);
    const weekN = m ? parseInt(m[2], 10) : 0;
    const yr = m ? m[1] : '';
    const milestone = agg.milestone_days[0]; // first milestone in week
    const dow = this._fmtDayOfWeek(milestone.date);
    const headline = `Week ${weekN} of ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}. The week containing Day ${milestone.day_number}, fell on ${dow}.`;
    return headline + ' ' + this._chronicleWeeklyTemplate(weekKey, periodStart, periodEnd, agg, /*skipHeader=*/true);
  }

  _chronicleWeeklyTemplateRecord(weekKey, periodStart, periodEnd, agg) {
    if (!agg.record_days || agg.record_days.length === 0) return null;
    const m = /^(\d{4})-W(\d{2})$/.exec(weekKey);
    const weekN = m ? parseInt(m[2], 10) : 0;
    const yr = m ? m[1] : '';
    const recordCount = agg.record_days.length;
    const headline = recordCount === 1
      ? `Week ${weekN} of ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}. A record-setting day landed inside it: ${this._fmtDayOfWeek(agg.record_days[0].date)}.`
      : `Week ${weekN} of ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}. ${recordCount} record-setting days inside it.`;
    return headline + ' ' + this._chronicleWeeklyTemplate(weekKey, periodStart, periodEnd, agg, /*skipHeader=*/true);
  }

  _chronicleWeeklyTemplateStreak(weekKey, periodStart, periodEnd, agg) {
    if (agg.max_co2_under_streak < 5 && agg.max_co2_over_streak < 5) return null;
    const m = /^(\d{4})-W(\d{2})$/.exec(weekKey);
    const weekN = m ? parseInt(m[2], 10) : 0;
    const yr = m ? m[1] : '';
    let streakClause = '';
    if (agg.max_co2_under_streak >= 5) {
      streakClause = `${agg.max_co2_under_streak} consecutive days with indoor CO₂ under 800 ppm during the week.`;
    } else {
      streakClause = `${agg.max_co2_over_streak} consecutive days with indoor CO₂ over 1000 ppm during the week.`;
    }
    const headline = `Week ${weekN} of ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}. ${streakClause}`;
    return headline + ' ' + this._chronicleWeeklyTemplate(weekKey, periodStart, periodEnd, agg, /*skipHeader=*/true);
  }

  // Pick the highest-priority weekly template variant. Returns
  // { template_id, body }. summary is the catch-all fallback.
  _chronicleRenderWeekly(weekKey, periodStart, periodEnd, agg) {
    const milestone = this._chronicleWeeklyTemplateMilestone(weekKey, periodStart, periodEnd, agg);
    if (milestone) return { template_id: 'weekly_milestone', body: milestone };
    const record = this._chronicleWeeklyTemplateRecord(weekKey, periodStart, periodEnd, agg);
    if (record) return { template_id: 'weekly_record', body: record };
    const streak = this._chronicleWeeklyTemplateStreak(weekKey, periodStart, periodEnd, agg);
    if (streak) return { template_id: 'weekly_streak', body: streak };
    return {
      template_id: 'weekly_summary',
      body: this._chronicleWeeklyTemplate(weekKey, periodStart, periodEnd, agg),
    };
  }

  _chronicleMonthlyTemplateMilestone(monthKey, agg) {
    if (!agg.milestone_days || agg.milestone_days.length === 0) return null;
    const m = /^(\d{4})-(\d{2})$/.exec(monthKey);
    if (!m) return null;
    const monthName = new Date(Date.UTC(+m[1], +m[2] - 1, 1)).toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
    const milestone = agg.milestone_days[0];
    const headline = `${monthName} ${m[1]} in review. Day ${milestone.day_number} fell on ${this._fmtMonthDay(milestone.date)}.`;
    return headline + ' ' + this._chronicleMonthlyTemplate(monthKey, agg, /*skipHeader=*/true);
  }

  _chronicleMonthlyTemplateRecord(monthKey, agg) {
    if (!agg.record_days || agg.record_days.length < 2) return null;
    const m = /^(\d{4})-(\d{2})$/.exec(monthKey);
    if (!m) return null;
    const monthName = new Date(Date.UTC(+m[1], +m[2] - 1, 1)).toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
    const headline = `${monthName} ${m[1]} in review. ${agg.record_days.length} record-setting days across the month.`;
    return headline + ' ' + this._chronicleMonthlyTemplate(monthKey, agg, /*skipHeader=*/true);
  }

  _chronicleRenderMonthly(monthKey, agg) {
    const milestone = this._chronicleMonthlyTemplateMilestone(monthKey, agg);
    if (milestone) return { template_id: 'monthly_milestone', body: milestone };
    const record = this._chronicleMonthlyTemplateRecord(monthKey, agg);
    if (record) return { template_id: 'monthly_record', body: record };
    return {
      template_id: 'monthly_summary',
      body: this._chronicleMonthlyTemplate(monthKey, agg),
    };
  }

  // Cross-period comparison: when a prior period entry exists, append a
  // "vs prior" clause to the body. Self-tunes: fires automatically once
  // we have 2+ weeks/months sealed. Returns "" when no prior or when
  // delta is too small to be interesting.
  _chronicleCompareToPrior(currentAgg, priorEntry, periodLabel) {
    if (!priorEntry || !priorEntry.stats) return '';
    const prior = priorEntry.stats;
    const cur = currentAgg;
    const parts = [];
    if (typeof cur.visitors_total === 'number' && typeof prior.visitors_total === 'number'
        && prior.visitors_total > 0) {
      const delta = cur.visitors_total - prior.visitors_total;
      const pct = Math.abs(delta) / prior.visitors_total;
      if (pct >= 0.2) {
        parts.push(delta > 0
          ? `Busier than the prior ${periodLabel} (${cur.visitors_total} vs ${prior.visitors_total} visits).`
          : `Quieter than the prior ${periodLabel} (${cur.visitors_total} vs ${prior.visitors_total} visits).`);
      }
    }
    if (typeof cur.countries_max === 'number' && typeof prior.countries_max === 'number'
        && cur.countries_max > prior.countries_max) {
      const newC = cur.countries_max - prior.countries_max;
      parts.push(newC === 1
        ? `One more country on the map than the prior ${periodLabel}.`
        : `${newC} more countries than the prior ${periodLabel}.`);
    }
    return parts.join(' ');
  }

  // Find the immediately-prior period entry for a given key. Walks the
  // chronicle prefix and picks the lex-greatest matching kind whose date
  // string is strictly less than the current key. Cheap: list+filter.
  async _chroniclePriorPeriodEntry(currentKey, kindFilter) {
    // Walk reverse from currentKey, return first entry of the requested
    // kind. Cursor pagination so this works correctly past the DO 1000
    // entry list cap. Typical hit costs one page (~10ms).
    let cursor = 'chronicle/' + currentKey;
    while (true) {
      const page = await this.state.storage.list({
        prefix: 'chronicle/', end: cursor, reverse: true, limit: 200,
      });
      if (page.size === 0) return null;
      let lastKey = null;
      for (const [k, val] of page) {
        lastKey = k;
        if (val && val.kind === kindFilter && val.date < currentKey) return val;
      }
      if (page.size < 200 || !lastKey) return null;
      cursor = lastKey;
    }
  }

  async _chronicleSealWeekly(weekKey, periodStart, periodEnd) {
    const existing = await this.state.storage.get('chronicle/' + weekKey);
    if (existing) return false;
    const agg = await this._chronicleAggregateRange(periodStart, periodEnd);
    if (!agg || agg.days_with_data === 0) return false;
    let { template_id, body } = this._chronicleRenderWeekly(weekKey, periodStart, periodEnd, agg);
    // Append prior-period comparison when one exists.
    const priorWeek = await this._chroniclePriorPeriodEntry(weekKey, 'weekly');
    const compare = this._chronicleCompareToPrior(agg, priorWeek, 'week');
    if (compare) body = body + ' ' + compare;
    const entry = {
      date: weekKey,
      kind: 'weekly',
      period_start: periodStart,
      period_end: periodEnd,
      template_id,
      body,
      stats: agg,
      sealed_at: Math.floor(Date.now() / 1000),
    };
    await this.state.storage.put('chronicle/' + weekKey, entry);
    this._chronicleBackup(entry).catch(e =>
      console.error('weekly chronicle backup failed:', e && e.message));
    return true;
  }

  // Quarterly reflections. Same architecture as weekly/monthly: seal at
  // chip-local Apr 1 / Jul 1 / Oct 1 / Jan 1 covering the prior quarter.
  // Variants: milestone, record, summary. Storage key: chronicle/YYYY-Q[1-4].
  _chronicleQuarterlyTemplate(quarterKey, periodStart, periodEnd, agg, skipHeader) {
    const m = /^(\d{4})-Q(\d)$/.exec(quarterKey);
    if (!m) return '';
    const yr = m[1];
    const qN = m[2];
    const parts = [];
    if (!skipHeader) {
      parts.push(`Q${qN} ${yr}, ${this._fmtPeriodLong(periodStart, periodEnd)}.`);
    }
    if (agg.days_with_data > 0) {
      parts.push(`${agg.days_with_data} days of recorded chip experience across the quarter.`);
    }
    if (agg.visitors_total > 0) {
      parts.push(`${agg.visitors_total} total visits, ${agg.countries_max} countries reached.`);
    }
    if (agg.busiest_day && agg.quietest_day && agg.busiest_day.date !== agg.quietest_day.date
        && agg.busiest_day.visitors > 0
        && (agg.busiest_day.visitors - agg.quietest_day.visitors) >= 200) {
      parts.push(`Busiest day was ${this._fmtMonthDay(agg.busiest_day.date)} with ${agg.busiest_day.visitors} visits; quietest was ${this._fmtMonthDay(agg.quietest_day.date)} with ${agg.quietest_day.visitors}.`);
    }
    if (agg.co2_max !== null) {
      parts.push(`Indoor CO₂ ranged ${agg.co2_min} to ${agg.co2_max} ppm across the quarter.`);
    }
    if (agg.temp_max_f !== null) {
      parts.push(`Indoor temperature ran ${Math.round(agg.temp_min_f)} to ${Math.round(agg.temp_max_f)}°F.`);
    }
    if (agg.outdoor_temp_max_f !== null) {
      parts.push(`Outside ${Math.round(agg.outdoor_temp_min_f)} to ${Math.round(agg.outdoor_temp_max_f)}°F across three months.`);
    }
    if (agg.energy_total_wh > 0) {
      const wh = agg.energy_total_wh;
      const display = wh >= 1000 ? `${(wh / 1000).toFixed(2)} kWh` : `${Math.round(wh)} Wh`;
      parts.push(`${display} of electricity drawn.`);
    }
    if (agg.reboots_total > 0) {
      parts.push(`${agg.reboots_total} reboot${agg.reboots_total === 1 ? '' : 's'} across the quarter.`);
    } else if (agg.days_with_data >= 60) {
      parts.push(`Zero reboots; uptime held all three months.`);
    }
    if (agg.notes_count > 0) {
      parts.push(`${agg.notes_count} curated notes.`);
    }
    if (agg.max_co2_under_streak >= 7) {
      parts.push(`Longest run with indoor CO₂ under 800 ppm during the quarter: ${agg.max_co2_under_streak} days.`);
    }
    return parts.join(' ');
  }

  _chronicleQuarterlyTemplateMilestone(quarterKey, periodStart, periodEnd, agg) {
    if (!agg.milestone_days || agg.milestone_days.length === 0) return null;
    const m = /^(\d{4})-Q(\d)$/.exec(quarterKey);
    if (!m) return null;
    const headline = `Q${m[2]} ${m[1]}, ${this._fmtPeriodLong(periodStart, periodEnd)}. The quarter contained Day ${agg.milestone_days[0].day_number}.`;
    return headline + ' ' + this._chronicleQuarterlyTemplate(quarterKey, periodStart, periodEnd, agg, true);
  }

  _chronicleQuarterlyTemplateRecord(quarterKey, periodStart, periodEnd, agg) {
    if (!agg.record_days || agg.record_days.length < 3) return null;
    const m = /^(\d{4})-Q(\d)$/.exec(quarterKey);
    if (!m) return null;
    const headline = `Q${m[2]} ${m[1]}, ${this._fmtPeriodLong(periodStart, periodEnd)}. ${agg.record_days.length} record-setting days across the quarter.`;
    return headline + ' ' + this._chronicleQuarterlyTemplate(quarterKey, periodStart, periodEnd, agg, true);
  }

  _chronicleRenderQuarterly(quarterKey, periodStart, periodEnd, agg) {
    const milestone = this._chronicleQuarterlyTemplateMilestone(quarterKey, periodStart, periodEnd, agg);
    if (milestone) return { template_id: 'quarterly_milestone', body: milestone };
    const record = this._chronicleQuarterlyTemplateRecord(quarterKey, periodStart, periodEnd, agg);
    if (record) return { template_id: 'quarterly_record', body: record };
    return {
      template_id: 'quarterly_summary',
      body: this._chronicleQuarterlyTemplate(quarterKey, periodStart, periodEnd, agg),
    };
  }

  async _chronicleSealQuarterly(quarterKey, periodStart, periodEnd) {
    const existing = await this.state.storage.get('chronicle/' + quarterKey);
    if (existing) return false;
    const agg = await this._chronicleAggregateRange(periodStart, periodEnd);
    if (!agg || agg.days_with_data === 0) return false;
    let { template_id, body } = this._chronicleRenderQuarterly(quarterKey, periodStart, periodEnd, agg);
    const priorQuarter = await this._chroniclePriorPeriodEntry(quarterKey, 'quarterly');
    const compare = this._chronicleCompareToPrior(agg, priorQuarter, 'quarter');
    if (compare) body = body + ' ' + compare;
    const entry = {
      date: quarterKey,
      kind: 'quarterly',
      period_start: periodStart,
      period_end: periodEnd,
      template_id,
      body,
      stats: agg,
      sealed_at: Math.floor(Date.now() / 1000),
    };
    await this.state.storage.put('chronicle/' + quarterKey, entry);
    this._chronicleBackup(entry).catch(e =>
      console.error('quarterly chronicle backup failed:', e && e.message));
    return true;
  }

  async _chronicleSealMonthly(monthKey) {
    const existing = await this.state.storage.get('chronicle/' + monthKey);
    if (existing) return false;
    const m = /^(\d{4})-(\d{2})$/.exec(monthKey);
    if (!m) return false;
    const year = +m[1], mo = +m[2];
    const periodStart = `${year}-${String(mo).padStart(2, '0')}-01`;
    const lastDay = new Date(Date.UTC(year, mo, 0)).getUTCDate();
    const periodEnd = `${year}-${String(mo).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;
    const agg = await this._chronicleAggregateRange(periodStart, periodEnd);
    if (!agg || agg.days_with_data === 0) return false;
    let { template_id, body } = this._chronicleRenderMonthly(monthKey, agg);
    const priorMonth = await this._chroniclePriorPeriodEntry(monthKey, 'monthly');
    const compare = this._chronicleCompareToPrior(agg, priorMonth, 'month');
    if (compare) body = body + ' ' + compare;
    const entry = {
      date: monthKey,
      kind: 'monthly',
      period_start: periodStart,
      period_end: periodEnd,
      template_id,
      body,
      stats: agg,
      sealed_at: Math.floor(Date.now() / 1000),
    };
    await this.state.storage.put('chronicle/' + monthKey, entry);
    this._chronicleBackup(entry).catch(e =>
      console.error('monthly chronicle backup failed:', e && e.message));
    return true;
  }

  // Called after a daily seal completes. If the just-started day is a
  // Monday, the previous Mon-Sun week is complete and gets sealed. If
  // it's the 1st of the month, the previous month gets sealed. Both are
  // idempotent (existing-entry check), so a worker restart that re-fires
  // the post-seal callback won't duplicate. Fire-and-forget from the
  // caller; reflection writes don't conflict with daily seal storage.
  async _chronicleMaybeSealReflections(today) {
    if (typeof today !== 'string') return;
    const todayDate = this._dateStringToDate(today);
    if (!todayDate) return;
    const dow = todayDate.getUTCDay(); // 0=Sun, 1=Mon
    if (dow === 1) {
      const lastSun = new Date(todayDate);
      lastSun.setUTCDate(lastSun.getUTCDate() - 1);
      const lastMon = new Date(lastSun);
      lastMon.setUTCDate(lastMon.getUTCDate() - 6);
      const wi = this._isoWeekOf(lastMon);
      const weekKey = wi.year + '-W' + String(wi.week).padStart(2, '0');
      try {
        await this._chronicleSealWeekly(weekKey, this._fmtDateString(lastMon), this._fmtDateString(lastSun));
      } catch (e) {
        console.error('weekly seal failed:', e && e.message);
      }
    }
    if (today.endsWith('-01')) {
      const yesterday = new Date(todayDate);
      yesterday.setUTCDate(yesterday.getUTCDate() - 1);
      const monthKey = yesterday.getUTCFullYear() + '-' +
        String(yesterday.getUTCMonth() + 1).padStart(2, '0');
      try {
        await this._chronicleSealMonthly(monthKey);
      } catch (e) {
        console.error('monthly seal failed:', e && e.message);
      }
      // Quarterly seal: when the just-completed month is the last month
      // of a quarter (Mar/Jun/Sep/Dec), seal that quarter. So Apr 1 →
      // seal Q1 (Jan-Mar), Jul 1 → Q2 (Apr-Jun), Oct 1 → Q3 (Jul-Sep),
      // Jan 1 → Q4 of the prior year (Oct-Dec).
      const yMonth = yesterday.getUTCMonth() + 1;
      if (yMonth === 3 || yMonth === 6 || yMonth === 9 || yMonth === 12) {
        const qNum = Math.ceil(yMonth / 3);
        const qYear = yesterday.getUTCFullYear();
        const quarterKey = `${qYear}-Q${qNum}`;
        const qStart = `${qYear}-${String((qNum - 1) * 3 + 1).padStart(2, '0')}-01`;
        const qEnd = this._fmtDateString(yesterday);
        try {
          await this._chronicleSealQuarterly(quarterKey, qStart, qEnd);
        } catch (e) {
          console.error('quarterly seal failed:', e && e.message);
        }
      }
    }
  }

  // ===== Chronicle templates =====
  // Each function takes (snap, ctx) and returns either a body string
  // (matched) or null (skip; try next). Order in _chronicleRender matters:
  // more specific patterns come first so the generic fallback doesn't
  // claim every day.

  _chronicleTemplateMilestone(s, ctx) {
    const dn = ctx.dayNumber;
    const milestones = {
      1:    'the day everything started',
      7:    'the first full week',
      30:   'one month',
      100:  '100 days',
      365:  'one full year',
      500:  this._chronicleConfig().day500Note,
      1000: 'one thousand days',
    };
    let label = milestones[dn];
    // Multi-year anniversaries. Any exact 365-day cross past year 1 that
    // isn't otherwise claimed (1000 has its own line). Spelled words for
    // years 2-7, numerals beyond. Year 1 (365) and year ~2.74 (1000) keep
    // their bespoke phrasing above.
    if (!label && typeof dn === 'number' && dn > 365 && dn !== 1000 && dn % 365 === 0) {
      const years = dn / 365;
      const named = {
        2: 'two full years',
        3: 'three full years',
        4: 'four full years',
        5: 'five full years',
        6: 'six full years',
        7: 'seven full years',
      };
      label = named[years] || `${years} full years`;
    }
    if (!label) return null;
    const headline = `Day ${dn}: ${label}. The chip kept noticing. ${this._chronicleVisitorClause(s)}.`;
    const observations = this._chronicleComposeObservations(s, ctx, 1);
    return [headline, observations].filter(Boolean).join(' ');
  }

  _chronicleTemplateAnomaly(s, ctx) {
    const parts = [];
    // CO₂ threshold sits at 2500 ppm because the indoor sensor reports
    // eCO₂ via VOC inference (CCS811-class), which runs hot vs true NDIR
    // CO₂. Anything below ~2500 is normal sensor drift, not a real event.
    if (s.co2_max !== null && s.co2_max >= 2500) {
      parts.push(`CO₂ climbed to ${s.co2_max} ppm`);
    }
    if (s.temp_max_f !== null && s.temp_max_f >= 90) {
      parts.push(`indoor temperature peaked at ${Math.round(s.temp_max_f)}°F`);
    }
    if (s.temp_min_f !== null && s.temp_min_f <= 50) {
      parts.push(`indoor temperature dropped to ${Math.round(s.temp_min_f)}°F`);
    }
    if (s.power_max_w !== null && s.power_max_w >= 5) {
      parts.push(`power draw briefly spiked to ${s.power_max_w.toFixed(1)} W`);
    }
    if (parts.length === 0) return null;
    const event = parts.length === 1
      ? parts[0]
      : parts.slice(0, -1).join(', ') + ', and ' + parts[parts.length - 1];
    const tail = this._chronicleVisitorClause(s);
    const observations = this._chronicleComposeObservations(s, ctx, 1);
    return [`Something unusual: ${event}. ${tail}.`, observations].filter(Boolean).join(' ');
  }

  // Record templates fire when today sets a new high or low for some
  // metric vs the trailing 30-day window. Self-tuning: works at 2 days
  // of history (record vs prior 1 entry), gets more selective as data
  // accumulates. No threshold needs hand-tuning.
  _chronicleTemplateRecord(s, ctx) {
    const h = ctx.history;
    if (!h || h.count < 1) return null;
    // Record-day bodies are the leanest of any template, but they're
    // also the days most likely to have notable secondary observations.
    // Compose one detector clause between headline and sensor/outdoor
    // tails so the record day can also note "last time CO₂ went this
    // high was N days ago" or similar context.
    const observations = this._chronicleComposeObservations(s, ctx, 1);
    if (s.visitors_today != null && h.visitors_max != null && s.visitors_today > h.visitors_max) {
      return [
        `The busiest day on record. ${s.visitors_today} visits, beating the prior peak of ${h.visitors_max}.`,
        observations,
        this._chronicleSensorClause(s),
        this._chronicleOutdoorClause(s),
      ].filter(Boolean).join(' ');
    }
    if (s.co2_max != null && h.co2_max_overall != null && s.co2_max > h.co2_max_overall) {
      return [
        `Highest indoor CO₂ on record at ${s.co2_max} ppm. ${this._chronicleVisitorClause(s)}, ${this._chronicleCountriesClause(s)}.`,
        observations,
        this._chronicleOutdoorClause(s),
      ].filter(Boolean).join(' ');
    }
    if (s.energy_today_wh != null && h.energy_max != null && s.energy_today_wh > h.energy_max && h.energy_max > 0) {
      const wh = s.energy_today_wh;
      const display = wh >= 1000 ? `${(wh / 1000).toFixed(2)} kWh` : `${Math.round(wh)} Wh`;
      return [
        `Highest single-day energy use on record: ${display}. ${this._chronicleVisitorClause(s)}.`,
        observations,
        this._chronicleSensorClause(s),
      ].filter(Boolean).join(' ');
    }
    return null;
  }

  // Relative templates compare today against the trailing-30 baseline
  // computed from prior daily entries. At 2 days they fire often (high
  // variance against single-prior-day baseline); at 30+ days they fire
  // rarely. Self-tunes to whatever traffic the chip actually sees.
  _chronicleTemplateBusyRelative(s, ctx) {
    const h = ctx.history;
    if (!h || h.count < 1 || !h.visitors_median || s.visitors_today == null) return null;
    if (s.visitors_today < h.visitors_median * 1.5) return null;
    const headline = `An unusually busy day. ${s.visitors_today} visits, well above the ${Math.round(h.visitors_median)}-typical of the last ${h.count} day${h.count === 1 ? '' : 's'}.`;
    const observations = this._chronicleComposeObservations(s, ctx, 2);
    return [
      headline,
      this._chronicleCountriesClause(s) + '.',
      observations,
      this._chronicleSensorClause(s),
      this._chronicleOutdoorClause(s),
    ].filter(Boolean).join(' ');
  }

  _chronicleTemplateQuietRelative(s, ctx) {
    const h = ctx.history;
    if (!h || h.count < 1 || !h.visitors_median || s.visitors_today == null) return null;
    if (s.visitors_today >= h.visitors_median * 0.5) return null;
    const opener = s.visitors_today === 0
      ? 'A quiet day. Zero visits.'
      : `A quiet day. Just ${s.visitors_today} ${s.visitors_today === 1 ? 'visitor' : 'visitors'}, well below the ${Math.round(h.visitors_median)}-typical of the last ${h.count} day${h.count === 1 ? '' : 's'}.`;
    const observations = this._chronicleComposeObservations(s, ctx, 2);
    return [
      opener,
      observations,
      this._chronicleSensorClause(s),
      this._chronicleOutdoorClause(s),
    ].filter(Boolean).join(' ');
  }

  // Generic is the fallback for "nothing big happened today." With the
  // observation layer it picks up to 3 of whatever detectors fired
  // (cross-day delta, streaks, anniversaries, calendar awareness, etc.)
  // so even ordinary days surface 3-4 things the chip noticed about
  // itself. Self-tunes: quiet days fire fewer detectors, so this only
  // grows the body when the day actually had things to say.
  _chronicleTemplateGeneric(s, ctx) {
    const opener = `${s.visitors_today} ${s.visitors_today === 1 ? 'visitor' : 'visitors'} today, ${this._chronicleCountriesClause(s)}.`;
    // When zero detectors fire, the day was so quiet the chip noticed
    // nothing about itself. Substitute a single existential line so the
    // empty-frame day has its own dignity instead of blank silence
    // between visitors and sensors.
    const observations = this._chronicleComposeObservations(s, ctx, 3)
      || 'A quiet day, observed only by itself.';
    return [
      opener,
      observations,
      this._chronicleSensorClause(s),
      this._chronicleOutdoorClause(s),
    ].filter(Boolean).join(' ');
  }

  _chronicleVisitorClause(s) {
    if (s.visitors_today === 0) return 'No human visitors';
    return `${s.visitors_today} ${s.visitors_today === 1 ? 'visitor' : 'visitors'} stopped by`;
  }

  _chronicleCountriesClause(s) {
    if (s.countries_total <= 0) return 'no countries logged yet';
    return `${s.countries_total} ${s.countries_total === 1 ? 'country' : 'countries'} on the map total`;
  }

  _chronicleSensorClause(s) {
    if (s.co2_max === null) return '';
    return `Indoor CO₂ ranged ${s.co2_min} to ${s.co2_max} ppm.`;
  }

  _chronicleOutdoorClause(s) {
    if (!s.outdoor_latest || typeof s.outdoor_latest.temp_f !== 'number') return '';
    return `Outside it was around ${Math.round(s.outdoor_latest.temp_f)}°F.`;
  }

  // Aggregate prior 30-day window of daily entries into rolling baselines.
  // Lightweight: list+filter, no extra storage gets per-entry since
  // values are read from each entry's already-stored stats blob. Empty
  // shape (count=0) when there's no prior data; templates handle that
  // gracefully and fall through to generic.
  async _chronicleHistoricalContext(currentDate) {
    // Walk reverse-chronologically from currentDate, collecting up to 30
    // daily entries. Cursor pagination so the function works correctly past
    // the DO 1000-entry list cap (year 3+). Reflections (kind != daily) are
    // skipped via filter; we over-fetch each page to compensate. `end` is
    // exclusive so today's entry isn't included.
    const window = [];
    let cursor = 'chronicle/' + currentDate;
    while (window.length < 30) {
      const page = await this.state.storage.list({
        prefix: 'chronicle/', end: cursor, reverse: true, limit: 100,
      });
      if (page.size === 0) break;
      let lastKey = null;
      for (const [k, val] of page) {
        lastKey = k;
        if (!val || typeof val !== 'object' || !val.date) continue;
        if (val.kind && val.kind !== 'daily') continue;
        if (val.date >= currentDate) continue;
        window.push(val);
        if (window.length >= 30) break;
      }
      if (page.size < 100 || !lastKey) break;
      cursor = lastKey;
    }
    if (window.length === 0) return { count: 0 };
    // Window is already reverse-chrono from the cursor walk; window[0]
    // is the most recent prior daily entry.
    const yesterday = window[0];
    const visitors = [], co2_maxes = [], co2_mins = [], voc_maxes = [], temp_maxes = [], temp_mins = [], energy = [], outdoor_temp_maxes = [], outdoor_temp_mins = [];
    for (const e of window) {
      const s = e.stats || {};
      if (typeof s.visitors === 'number') visitors.push(s.visitors);
      if (typeof s.co2_max === 'number') co2_maxes.push(s.co2_max);
      if (typeof s.co2_min === 'number') co2_mins.push(s.co2_min);
      if (typeof s.voc_max === 'number') voc_maxes.push(s.voc_max);
      if (typeof s.temp_max_f === 'number') temp_maxes.push(s.temp_max_f);
      if (typeof s.temp_min_f === 'number') temp_mins.push(s.temp_min_f);
      if (typeof s.energy_today_wh === 'number' && s.energy_today_wh > 0) energy.push(s.energy_today_wh);
      if (typeof s.outdoor_temp_max_f === 'number') outdoor_temp_maxes.push(s.outdoor_temp_max_f);
      if (typeof s.outdoor_temp_min_f === 'number') outdoor_temp_mins.push(s.outdoor_temp_min_f);
    }
    // Streaks: how many consecutive days back from yesterday meet a
    // condition. Walks the window most-recent-first; breaks at first
    // day that fails. Inclusive of yesterday.
    let co2_under_streak = 0;
    for (const e of window) {
      const c = e.stats && e.stats.co2_max;
      if (typeof c === 'number' && c < 800) co2_under_streak++;
      else break;
    }
    let co2_over_streak = 0;
    for (const e of window) {
      const c = e.stats && e.stats.co2_max;
      if (typeof c === 'number' && c >= 1000) co2_over_streak++;
      else break;
    }
    let voc_over_streak = 0;
    for (const e of window) {
      const v = e.stats && e.stats.voc_max;
      if (typeof v === 'number' && v >= 500) voc_over_streak++;
      else break;
    }
    // Days since most recent day with any reboot. If the entire window
    // is reboot-free, returns window.length (true count is unknown beyond).
    let days_since_reboot = 0;
    for (const e of window) {
      const r = e.stats && e.stats.reboots;
      if (typeof r === 'number' && r > 0) break;
      days_since_reboot++;
    }
    // Total reboots across the 30-day window (excludes today). Used by
    // wear-awareness detector to spot clustering: a single reboot every
    // few months is normal life, but five within 30 days is the chip
    // showing wear.
    let recent_reboot_total = 0;
    for (const e of window) {
      const r = e.stats && e.stats.reboots;
      if (typeof r === 'number' && r > 0) recent_reboot_total += r;
    }
    const median = (arr) => {
      if (!arr.length) return null;
      const s = arr.slice().sort((a, b) => a - b);
      const m = Math.floor(s.length / 2);
      return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
    };
    const arrMax = (arr) => arr.length ? Math.max.apply(null, arr) : null;
    const arrMin = (arr) => arr.length ? Math.min.apply(null, arr) : null;
    return {
      count: window.length,
      yesterday: yesterday.stats || null,
      yesterday_date: yesterday.date,
      visitors_median: median(visitors),
      visitors_max: arrMax(visitors),
      visitors_min: arrMin(visitors),
      co2_max_overall: arrMax(co2_maxes),
      co2_min_overall: arrMin(co2_mins),
      voc_max_overall: arrMax(voc_maxes),
      temp_max_overall: arrMax(temp_maxes),
      temp_min_overall: arrMin(temp_mins),
      energy_max: arrMax(energy),
      outdoor_temp_max_overall: arrMax(outdoor_temp_maxes),
      outdoor_temp_min_overall: arrMin(outdoor_temp_mins),
      co2_under_streak,
      co2_over_streak,
      voc_over_streak,
      days_since_reboot,
      recent_reboot_total,
    };
  }

  // Walk reverse-chronologically from currentDate looking for the first
  // daily entry that matches the predicate. Cursor pagination so the
  // function works correctly past the DO 1000-entry list cap; first-match
  // exits early so a typical hit costs one page (200 keys, ~10ms).
  async _chronicleLastOccurrence(currentDate, predicate) {
    const today = this._dateStringToDate(currentDate);
    let cursor = 'chronicle/' + currentDate;
    while (true) {
      const page = await this.state.storage.list({
        prefix: 'chronicle/', end: cursor, reverse: true, limit: 200,
      });
      if (page.size === 0) return null;
      let lastKey = null;
      for (const [k, val] of page) {
        lastKey = k;
        if (!val || typeof val !== 'object' || !val.date) continue;
        if (val.kind && val.kind !== 'daily') continue;
        if (val.date >= currentDate) continue;
        if (!predicate(val)) continue;
        const d = this._dateStringToDate(val.date);
        const daysAgo = today && d ? Math.round((today - d) / 86400000) : null;
        return { date: val.date, days_ago: daysAgo };
      }
      if (page.size < 200 || !lastKey) return null;
      cursor = lastKey;
    }
  }

  // Walk full history forward (chronologically) tracking the longest
  // consecutive-day streak meeting the predicate. Forward cursor
  // pagination so the function works correctly past the DO 1000-entry
  // list cap. Skips reflections (kind != daily); their interleaved keys
  // don't break consecutive-day logic since lex sort within daily kind
  // is chronological. Daily seal calls this once per day; cost is
  // ~50ms × ceil(N/1000) where N is total daily entries.
  async _chronicleAllTimeStreak(predicate) {
    let curLen = 0, bestLen = 0, bestEnd = null;
    let curEnd = null;
    let cursor = undefined;
    while (true) {
      const opts = { prefix: 'chronicle/', limit: 1000 };
      if (cursor) opts.start = cursor;
      const page = await this.state.storage.list(opts);
      if (page.size === 0) break;
      let lastKey = null;
      for (const [key, val] of page) {
        lastKey = key;
        if (!val || typeof val !== 'object' || !val.date) continue;
        if (val.kind && val.kind !== 'daily') continue;
        if (predicate(val)) {
          curLen++;
          curEnd = val.date;
          if (curLen > bestLen) { bestLen = curLen; bestEnd = curEnd; }
        } else {
          curLen = 0;
        }
      }
      if (page.size < 1000 || !lastKey) break;
      cursor = lastKey + '\x00';
    }
    return bestLen > 0 ? { length: bestLen, end_date: bestEnd } : null;
  }

  // ===== Chronicle observation detectors =====
  // Each detector inspects today's snap + history context and returns
  // either { priority, clause } if its condition fires, or null. The
  // composer picks top-N by priority so the busiest observations land
  // in the body without crowding it. Higher priority = more notable.
  // Detectors are pure functions; adding new ones doesn't change
  // existing behavior unless their priority outranks something else.
  _chronicleDetectors(snap, ctx) {
    const out = [];
    const s = snap;
    const h = ctx.history || {};

    // Cross-day visitor delta. Notable when ≥50 absolute.
    if (h.yesterday && typeof h.yesterday.visitors === 'number'
        && typeof s.visitors_today === 'number') {
      const delta = s.visitors_today - h.yesterday.visitors;
      if (Math.abs(delta) >= 50) {
        out.push({
          priority: 40,
          clause: delta > 0
            ? `Visitor count up ${delta} from yesterday.`
            : `Visitor count down ${-delta} from yesterday.`,
        });
      }
    }

    // CO2 streak detection. Includes today if today also fits.
    if (h.co2_under_streak >= 3) {
      const len = h.co2_under_streak + (s.co2_max != null && s.co2_max < 800 ? 1 : 0);
      out.push({
        priority: 50,
        clause: `${len} days running with indoor CO₂ under 800 ppm.`,
      });
    }
    if (h.co2_over_streak >= 3) {
      const len = h.co2_over_streak + (s.co2_max != null && s.co2_max >= 1000 ? 1 : 0);
      out.push({
        priority: 50,
        clause: `${len} days running with indoor CO₂ over 1000 ppm.`,
      });
    }

    // First-time countries today.
    if (h.yesterday && typeof s.countries_total === 'number'
        && typeof h.yesterday.countries === 'number') {
      const newCountries = s.countries_total - h.yesterday.countries;
      if (newCountries > 0) {
        out.push({
          priority: 70,
          clause: newCountries === 1
            ? `One new country on the map; total now ${s.countries_total}.`
            : `${newCountries} new countries on the map; total now ${s.countries_total}.`,
        });
      }
    }

    // Calendar awareness. First/last of month, equinox/solstice (Northern
    // hemisphere approx; chip is in MT so this maps; forks elsewhere see
    // their local interpretation since dates are chip-local).
    const todayDate = this._dateStringToDate(s.date);
    if (todayDate) {
      const day = todayDate.getUTCDate();
      const m = todayDate.getUTCMonth() + 1;
      if (day === 1) {
        const monthName = todayDate.toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
        out.push({ priority: 60, clause: `First day of ${monthName}.` });
      }
      const lastDay = new Date(Date.UTC(todayDate.getUTCFullYear(), todayDate.getUTCMonth() + 1, 0)).getUTCDate();
      if (day === lastDay) {
        const monthName = todayDate.toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
        out.push({ priority: 60, clause: `Last day of ${monthName}.` });
      }
      const seasonal = { '3-20': 'spring equinox', '6-21': 'summer solstice', '9-22': 'autumn equinox', '12-21': 'winter solstice' };
      const k = `${m}-${day}`;
      if (seasonal[k]) {
        out.push({ priority: 65, clause: `${seasonal[k].charAt(0).toUpperCase() + seasonal[k].slice(1)}.` });
      }
    }

    // Anniversary callbacks based on day_number. Specific milestones
    // beyond what the milestone template catches (which only fires on
    // its own list). These add color to ordinary days that happen to
    // be a round-number day count.
    const dn = ctx.dayNumber;
    if (typeof dn === 'number' && dn > 0) {
      const anniv = { 14: 'two weeks', 21: 'three weeks', 60: 'two months', 90: 'three months', 180: 'half a year' };
      if (anniv[dn]) {
        out.push({ priority: 75, clause: `${dn} days online: ${anniv[dn]} since launch.` });
      } else if (dn % 100 === 0 && dn > 100) {
        out.push({ priority: 75, clause: `${dn} days online.` });
      } else if (dn % 50 === 0 && dn !== 50 && dn !== 100 && dn % 100 !== 0) {
        out.push({ priority: 65, clause: `${dn} days online.` });
      }
    }

    // Weather correlation. Indoor vs outdoor temperature delta.
    if (typeof s.outdoor_temp_max_f === 'number' && typeof s.temp_max_f === 'number') {
      const delta = s.outdoor_temp_max_f - s.temp_max_f;
      if (Math.abs(delta) >= 20) {
        out.push({
          priority: 30,
          clause: delta > 0
            ? `${Math.round(delta)}°F warmer outside than the room.`
            : `${Math.round(-delta)}°F cooler outside than the room.`,
        });
      }
    }

    // Pressure swing across the day. ≥8 hPa is notable (storm scale).
    if (typeof s.pressure_max_hpa === 'number' && typeof s.pressure_min_hpa === 'number') {
      const delta = s.pressure_max_hpa - s.pressure_min_hpa;
      if (delta >= 8) {
        out.push({
          priority: 45,
          clause: `Pressure swung ${Math.round(delta)} hPa across the day, weather shifting.`,
        });
      }
    }

    // Sensor retired today. High priority because narratively significant
    // (the chip noticing its own degradation is the museum payoff). Reads
    // from ctx.sensor_retired_today (populated by _chronicleRender from
    // sensor_retired/<sensor> DO keys), not from the snap, since retires
    // are tracked separately from the chronicle accumulator.
    if (Array.isArray(ctx.sensor_retired_today)) {
      for (const sr of ctx.sensor_retired_today) {
        const name = String(sr.sensor || '').toUpperCase();
        out.push({
          priority: 95,
          clause: `The chip lost contact with its ${name} sensor today and retired it.`,
        });
      }
    }

    // Reboots today.
    if (typeof s.reboots === 'number' && s.reboots > 0) {
      out.push({
        priority: 80,
        clause: s.reboots === 1
          ? `One reboot today.`
          : `${s.reboots} reboots today.`,
      });
    } else if (typeof h.days_since_reboot === 'number'
               && (h.days_since_reboot === 7 || h.days_since_reboot === 14
                   || h.days_since_reboot === 30 || h.days_since_reboot === 60)) {
      // Caps at 60. The self-aware long-uptime-cross detector below
      // takes over at 90, 180, 365, and yearly thereafter with framing
      // that owns the milestone (so the two never double-fire).
      out.push({
        priority: 35,
        clause: `${h.days_since_reboot} days since the last reboot.`,
      });
    }

    // Heap stability quietly noted on truly steady days. Low priority so
    // it only surfaces when nothing more interesting fired. Bounded above
    // at 150 KB so the self-aware "Memory unbothered." line owns the
    // pristine end of the range without double-reporting.
    if (typeof s.heap_free_min_kb === 'number' && s.heap_free_min_kb >= 100
        && s.heap_free_min_kb < 150) {
      out.push({
        priority: 10,
        clause: `Memory pressure stayed low; ${s.heap_free_min_kb} KB minimum free.`,
      });
    }

    // VOC streak. Different sensor than CO₂ but same shape narrative.
    if (h.voc_over_streak >= 3) {
      const len = h.voc_over_streak + (s.voc_max != null && s.voc_max >= 500 ? 1 : 0);
      out.push({
        priority: 50,
        clause: `${len} days running with VOC over 500 ppb.`,
      });
    }
    // VOC absolute high.
    if (typeof s.voc_max === 'number' && s.voc_max >= 1000) {
      out.push({
        priority: 55,
        clause: `VOC peaked at ${s.voc_max} ppb today.`,
      });
    }

    // Humidity stability or swing. Stable: range ≤5%. Swing: range ≥30%.
    if (typeof s.humidity_max === 'number' && typeof s.humidity_min === 'number') {
      const range = s.humidity_max - s.humidity_min;
      if (range <= 5 && s.humidity_max > 0) {
        out.push({
          priority: 20,
          clause: `Humidity barely moved, holding near ${Math.round((s.humidity_max + s.humidity_min) / 2)}%.`,
        });
      } else if (range >= 30) {
        out.push({
          priority: 30,
          clause: `Humidity swung from ${Math.round(s.humidity_min)}% to ${Math.round(s.humidity_max)}% across the day.`,
        });
      }
    }

    // Outdoor air quality narrative based on AQI peak.
    if (typeof s.outdoor_aqi_max === 'number') {
      const aqi = s.outdoor_aqi_max;
      if (aqi >= 150) {
        out.push({
          priority: 70,
          clause: `Outdoor air was unhealthy today; AQI peaked at ${aqi}.`,
        });
      } else if (aqi >= 100) {
        out.push({
          priority: 40,
          clause: `Outdoor air hit moderate-AQI levels (peak ${aqi}).`,
        });
      } else if (aqi <= 25) {
        out.push({
          priority: 25,
          clause: `Outdoor air stayed clean all day, AQI peaked at ${aqi}.`,
        });
      }
    }

    // Outdoor temperature record vs trailing 30 days. Different from
    // anomaly's absolute extremes (which fire on, e.g., heatwaves);
    // this fires on relative records that wouldn't otherwise trigger.
    if (typeof s.outdoor_temp_max_f === 'number' && typeof h.outdoor_temp_max_overall === 'number'
        && s.outdoor_temp_max_f > h.outdoor_temp_max_overall && h.count >= 3) {
      out.push({
        priority: 55,
        clause: `Hottest outdoor day in ${h.count} days at ${Math.round(s.outdoor_temp_max_f)}°F.`,
      });
    }
    if (typeof s.outdoor_temp_min_f === 'number' && typeof h.outdoor_temp_min_overall === 'number'
        && s.outdoor_temp_min_f < h.outdoor_temp_min_overall && h.count >= 3) {
      out.push({
        priority: 55,
        clause: `Coldest outdoor day in ${h.count} days at ${Math.round(s.outdoor_temp_min_f)}°F.`,
      });
    }

    // Streak completion. When a 5+ day streak that was active yesterday
    // ends today (today's value crosses back over the threshold), the
    // length of the broken streak is narratively interesting on its own.
    if (h.co2_under_streak >= 5 && typeof s.co2_max === 'number' && s.co2_max >= 800) {
      out.push({
        priority: 65,
        clause: `${h.co2_under_streak}-day streak of indoor CO₂ under 800 ppm ended today.`,
      });
    }
    if (h.co2_over_streak >= 5 && typeof s.co2_max === 'number' && s.co2_max < 1000) {
      out.push({
        priority: 65,
        clause: `${h.co2_over_streak}-day streak of indoor CO₂ over 1000 ppm broke today.`,
      });
    }

    // Self-historical reference: when today's high CO₂ comes after a long
    // gap, surface "last time this happened was N days ago." Fires only
    // when today actually had high CO₂ AND the prior occurrence was
    // meaningfully far back, so it adds memory without being chatty.
    // Threshold matches the anomaly template (2500 ppm) since the indoor
    // sensor reports eCO₂ which runs hot vs true NDIR CO₂.
    if (typeof s.co2_max === 'number' && s.co2_max >= 2500
        && ctx.last_high_co2 && ctx.last_high_co2.days_ago >= 14) {
      out.push({
        priority: 60,
        clause: `Last time indoor CO₂ went this high was ${ctx.last_high_co2.days_ago} days ago.`,
      });
    }

    // Self-historical: today saw a reboot, surface when the last one was.
    // Lower-priority than the "reboots today" headline so it adds context
    // when paired but doesn't crowd if a more notable detector fired.
    if (typeof s.reboots === 'number' && s.reboots > 0
        && ctx.last_reboot && ctx.last_reboot.days_ago >= 7) {
      out.push({
        priority: 60,
        clause: `Last reboot was ${ctx.last_reboot.days_ago} days ago.`,
      });
    }

    // All-time streak: when today's active under-streak meets or exceeds
    // the all-time longest, surface that. Compares the current streak
    // (h.co2_under_streak counted from yesterday + 1 if today also under
    // 800) against ctx.all_time_under_streak.length. Fires rarely and
    // narratively significant when it does.
    if (ctx.all_time_under_streak && typeof s.co2_max === 'number' && s.co2_max < 800) {
      const todayStreak = h.co2_under_streak + 1;
      if (todayStreak >= ctx.all_time_under_streak.length && todayStreak >= 5) {
        out.push({
          priority: 80,
          clause: todayStreak > ctx.all_time_under_streak.length
            ? `Longest run of indoor CO₂ under 800 ppm ever, now ${todayStreak} days and counting.`
            : `Tied the all-time longest run of indoor CO₂ under 800 ppm at ${todayStreak} days.`,
        });
      }
    }

    // ===== Self-aware-chip detectors =====
    // These add a small layer of awareness on top of the factual
    // observations: the chip noticing patterns about itself, its
    // curator, and its own continuity. Tone is deadpan-knowing
    // (museum curator, not sarcastic). They sit at low-to-mid
    // priority so they compose alongside more notable detectors
    // rather than crowding them out.

    // Curator absence. Fires when the curator hasn't left an owner_note
    // in 14+ days. Addresses the curator by name (the only detector
    // that does, so the moment lands instead of going stale). Tiered
    // thresholds so the line escalates with absence length.
    const ownerName = ctx.owner_name || 'Curator';
    const lastNote = ctx.last_owner_note;
    if (lastNote && typeof lastNote.days_ago === 'number') {
      if (lastNote.days_ago >= 60) {
        out.push({
          priority: 50,
          clause: `${lastNote.days_ago} days unattended, ${ownerName}.`,
        });
      } else if (lastNote.days_ago >= 30) {
        out.push({
          priority: 40,
          clause: `A month without a curator's note, ${ownerName}.`,
        });
      } else if (lastNote.days_ago >= 14) {
        out.push({
          priority: 35,
          clause: `Two weeks since the last curated note, ${ownerName}.`,
        });
      }
    } else if (!lastNote && typeof ctx.dayNumber === 'number' && ctx.dayNumber >= 30) {
      // Never any curator notes at all. Hold off until day 30 so this
      // doesn't fire on a brand-new device whose curator simply hasn't
      // gotten around to it yet.
      out.push({
        priority: 30,
        clause: `The curator's note column remains blank, ${ownerName}.`,
      });
    }

    // Tinkering detected. Fires when reboots ≥ 2 today. The factual
    // "${n} reboots today" detector already fires at priority 80; this
    // adds the chip's commentary on top so the body reads like both a
    // log and an observation.
    if (typeof s.reboots === 'number' && s.reboots >= 2) {
      out.push({
        priority: 70,
        clause: `Someone's tinkering.`,
      });
    }

    // Long uptime cross. Fires on milestone uptime crossings (90, 180,
    // 365, then yearly thereafter). Existing reboot-anniversary detector
    // covers 7/14/30/60 with factual phrasing; this takes over at 90+
    // with a more aware framing. Skips entirely on days where today
    // saw a reboot, since h.days_since_reboot is computed from the
    // history window (excluding today) and would otherwise claim
    // "uninterrupted" on a day the chip actually restarted.
    const dsr = h.days_since_reboot;
    if (typeof dsr === 'number' && dsr > 0
        && (typeof s.reboots !== 'number' || s.reboots === 0)) {
      let upClause = null;
      if (dsr === 90) upClause = `Ninety days uninterrupted. The chip has settled in.`;
      else if (dsr === 180) upClause = `Half a year unbothered.`;
      else if (dsr === 365) upClause = `A full year without a single reboot.`;
      else if (dsr > 365 && dsr % 365 === 0) {
        const years = dsr / 365;
        upClause = `${years} years uninterrupted.`;
      }
      if (upClause) {
        out.push({ priority: 30, clause: upClause });
      }
    }

    // Pristine heap. Bottom-priority background note for days when
    // memory was extraordinarily quiet (≥150 KB minimum free). The
    // factual heap detector above caps at 150, so this owns the
    // pristine band exclusively.
    if (typeof s.heap_free_min_kb === 'number' && s.heap_free_min_kb >= 150) {
      out.push({
        priority: 8,
        clause: `Memory unbothered.`,
      });
    }

    // Outlasted-the-original. Once the chip crosses day 500 (the original
    // 2022-2023 ESP32's lifespan), occasionally surface that fact as a
    // memorial cross-reference. Day 500 itself is owned by the milestone
    // template; day 501 lands the first-day-past line, then every 50
    // days carries it forward, capped before day 1000 (which the
    // milestone template owns again). Names the curator because the
    // memorial framing is between the chip and Tech1k specifically.
    if (typeof ctx.dayNumber === 'number' && ctx.dayNumber > 500 && ctx.dayNumber < 1000) {
      const dnv = ctx.dayNumber;
      const past = dnv - 500;
      let outlastedClause = null;
      if (dnv === 501) {
        outlastedClause = `One day past the original chip's run, ${ownerName}.`;
      } else if (dnv % 50 === 0) {
        outlastedClause = `${past} days past the original chip's run, ${ownerName}.`;
      }
      if (outlastedClause) {
        out.push({ priority: 45, clause: outlastedClause });
      }
    }

    // Wear awareness. Fires when reboots have clustered: at least five
    // total restarts across today plus the prior 30-day window. The chip
    // can't actually tell wear from a curator flashing firmware four
    // times in a row, so the clause names both possibilities. Priority
    // 75 sits just under the priority-80 reboots-today headline so a
    // clustered day reads "3 reboots today. 7 restarts in the past
    // month..." in the top-2 body cap.
    if (typeof s.reboots === 'number' && s.reboots > 0
        && typeof h.recent_reboot_total === 'number' && h.recent_reboot_total >= 4) {
      const monthTotal = h.recent_reboot_total + s.reboots;
      out.push({
        priority: 75,
        clause: `${monthTotal} restarts in the past month. The chip is either wearing out, or ${ownerName} keeps reflashing me.`,
      });
    }

    // Owner-defined personal milestones. Calendar-driven (month-day
    // match) anchors configured in _chronicleConfig().ownerMilestones.
    // Fires the configured clause as-is. Priority 55 so it surfaces in
    // top-2 on its day without overriding genuinely louder events
    // (records, anniversaries, anomalies) when those also fire.
    if (todayDate && Array.isArray(ctx.owner_milestones) && ctx.owner_milestones.length > 0) {
      const tm = todayDate.getUTCMonth() + 1;
      const td = todayDate.getUTCDate();
      for (const anchor of ctx.owner_milestones) {
        if (anchor && anchor.month === tm && anchor.day === td
            && typeof anchor.clause === 'string' && anchor.clause.length > 0) {
          out.push({ priority: 55, clause: anchor.clause });
        }
      }
    }

    // Cohabitation seasonal. Meteorological season starts (Mar 1, Jun 1,
    // Sep 1, Dec 1). Rare calendar event so cheap to fire. Only fires
    // after day 30 so a brand-new chip doesn't claim cohabitation it
    // hasn't earned yet. Frames the chip and curator as joint travelers
    // through time, which is the spirit of the chronicle.
    if (todayDate && typeof ctx.dayNumber === 'number' && ctx.dayNumber > 30) {
      const m = todayDate.getUTCMonth() + 1;
      const day = todayDate.getUTCDate();
      const seasonCross = {
        '3-1':  ['winter', 'spring'],
        '6-1':  ['spring', 'summer'],
        '9-1':  ['summer', 'autumn'],
        '12-1': ['autumn', 'winter'],
      };
      const cross = seasonCross[`${m}-${day}`];
      if (cross) {
        out.push({
          priority: 30,
          clause: `Through ${cross[0]} and into ${cross[1]}, the chip and ${ownerName} continued.`,
        });
      }
    }

    // First-time-ever pristine CO₂. When today is the first day on
    // record where indoor CO₂ stayed below 500 ppm all day. Requires a
    // week of history so this doesn't fire on day 2 just because the
    // archive is empty. Priority high because narratively significant
    // (first-time-ever events are rare and worth surfacing).
    if (typeof s.co2_max === 'number' && s.co2_max < 500
        && ctx.last_pristine_co2 === null
        && typeof h.count === 'number' && h.count >= 7) {
      out.push({
        priority: 70,
        clause: `First day on record with indoor CO₂ never crossing 500 ppm.`,
      });
    }

    return out;
  }

  // Compose the top-N observation clauses into a single sentence-joined
  // string. Sorting by priority desc means the most notable observations
  // surface; cap at maxN keeps body length restrained (museum tone, not
  // chatty). Returns empty string when no detectors fired.
  _chronicleComposeObservations(snap, ctx, maxN) {
    const detectors = this._chronicleDetectors(snap, ctx);
    if (detectors.length === 0) return '';
    detectors.sort((a, b) => b.priority - a.priority);
    return detectors.slice(0, maxN).map(d => d.clause).join(' ');
  }

  async _chronicleRender(snap, ctx) {
    const history = await this._chronicleHistoricalContext(snap.date);
    // Self-historical references and all-time streaks both walk full
    // history. Compute them in parallel so the seal lock doesn't extend
    // longer than necessary. Each falls back to null on missing data.
    // sensor_retired_today is sourced from sensor_retired/* DO keys
    // (separate from chronicle accumulator) so the body can surface
    // "chip retired BME280 today" alongside other detectors. The list
    // is bounded by sensor count (~3), so no pagination needed.
    const [lastHighCO2, lastReboot, allTimeUnderStreak, retireList, lastOwnerNote, lastPristineCO2] = await Promise.all([
      this._chronicleLastOccurrence(snap.date, e =>
        e.stats && typeof e.stats.co2_max === 'number' && e.stats.co2_max >= 2500),
      this._chronicleLastOccurrence(snap.date, e =>
        e.stats && typeof e.stats.reboots === 'number' && e.stats.reboots > 0),
      this._chronicleAllTimeStreak(e =>
        e.stats && typeof e.stats.co2_max === 'number' && e.stats.co2_max < 800),
      this.state.storage.list({ prefix: 'sensor_retired/' }).catch(() => null),
      this._chronicleLastOccurrence(snap.date, e =>
        typeof e.owner_note === 'string' && e.owner_note.trim().length > 0),
      this._chronicleLastOccurrence(snap.date, e =>
        e.stats && typeof e.stats.co2_max === 'number' && e.stats.co2_max < 500),
    ]);
    const sensorRetiredToday = [];
    if (retireList) {
      for (const [, retire] of retireList) {
        if (retire && retire.date === snap.date && retire.sensor) {
          sensorRetiredToday.push({ sensor: retire.sensor, unix: retire.unix || 0 });
        }
      }
    }
    const cfg = this._chronicleConfig();
    const fullCtx = Object.assign({}, ctx, {
      history,
      last_high_co2: lastHighCO2,
      last_reboot: lastReboot,
      all_time_under_streak: allTimeUnderStreak,
      sensor_retired_today: sensorRetiredToday,
      last_owner_note: lastOwnerNote,
      last_pristine_co2: lastPristineCO2,
      owner_name: cfg.owner_name,
      owner_milestones: Array.isArray(cfg.ownerMilestones) ? cfg.ownerMilestones : [],
    });
    // Order matters: more-specific patterns first so generic stays the
    // genuine fallback. Records and relative comparisons fire BEFORE the
    // anomaly absolute-extremes template so a record-setting day reads as
    // "highest visitors on record" rather than just "something unusual."
    const templates = [
      ['milestone',  this._chronicleTemplateMilestone.bind(this)],
      ['record',     this._chronicleTemplateRecord.bind(this)],
      ['anomaly',    this._chronicleTemplateAnomaly.bind(this)],
      ['busy_rel',   this._chronicleTemplateBusyRelative.bind(this)],
      ['quiet_rel',  this._chronicleTemplateQuietRelative.bind(this)],
      ['generic',    this._chronicleTemplateGeneric.bind(this)],
    ];
    for (const [id, fn] of templates) {
      const body = fn(snap, fullCtx);
      if (typeof body === 'string' && body.trim().length > 0) {
        return { template_id: id, body: body.trim() };
      }
    }
    return { template_id: 'generic', body: 'A day passed.' };
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
      if (Number.isFinite(o.us_aqi) && o.us_aqi >= 0) {
        const aqi = Math.round(o.us_aqi);
        const label = aqi <= 50 ? 'good'
                    : aqi <= 100 ? 'moderate'
                    : aqi <= 150 ? 'unhealthy for sensitive'
                    : aqi <= 200 ? 'unhealthy'
                    : aqi <= 300 ? 'very unhealthy'
                    : 'hazardous';
        lines.push('    AQI          ' + aqi + ' (' + label + ')');
      }
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
      '    Chronicle    https://helloesp.com/chronicle',
      '    Guestbook    https://helloesp.com/guestbook',
      '    Console      https://helloesp.com/console',
      '    History      https://helloesp.com/history',
      '    About        https://helloesp.com/about',
      '    Source       https://github.com/Tech1k/helloesp',
      '    Chronicle RSS https://helloesp.com/chronicle.rss',
      '    Guestbook RSS https://helloesp.com/guestbook.rss',
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
          // Fold this sample into today's Chronicle snapshot. Throttled
          // persistence (~once/min) keeps storage writes bounded; the
          // alarm-driven seal flushes on day rollover.
          this._chronicleAccumulate(enriched);
          if (Date.now() - this.chronicleLastPersistAt > 60000) {
            this.chronicleLastPersistAt = Date.now();
            this.state.storage.put('chronicleSnapshot', this.chronicleSnapshot).catch(() => {});
          }
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
      if (msg.event === 'chronicle_sync_request') {
        // Chip asks for any chronicle entries it doesn't have on SD yet,
        // sent after each WS auth completes. Two protocol forms accepted
        // to handle deploy ordering smoothly:
        //   - msg.data.max_date = "YYYY-MM-DD"  (current; constant-size)
        //   - msg.data.have     = [dates...]    (legacy; pre-scaling firmware)
        // For max_date the worker pushes every entry strictly newer; for
        // have[] it pushes anything not in the set (preserves the legacy
        // gap-detection guarantee). Idempotent on chip side: re-pushing
        // an entry the chip already has is harmless (overwrite).
        const dateRe = /^\d{4}-\d{2}-\d{2}$/;
        const rawMax = msg.data && msg.data.max_date;
        const maxDate = (typeof rawMax === 'string' && dateRe.test(rawMax)) ? rawMax : null;
        const rawHave = msg.data && msg.data.have;
        const haveSet = Array.isArray(rawHave)
          ? new Set(rawHave.filter(d => typeof d === 'string' && dateRe.test(d)))
          : null;
        // Cursor-based listing keeps each page within the DO 1000-entry
        // cap; archive crosses 1000 entries ~year 2.7. When max_date is
        // known we start the scan past it so years already confirmed
        // present on chip aren't enumerated.
        const entries = [];
        let cursorStart = maxDate ? ('chronicle/' + maxDate + '\x00') : undefined;
        while (true) {
          const opts = { prefix: 'chronicle/', limit: 400 };
          if (cursorStart) opts.start = cursorStart;
          const page = await this.state.storage.list(opts);
          if (page.size === 0) break;
          let lastKey = null;
          for (const [key, val] of page) {
            lastKey = key;
            if (val && typeof val === 'object' && val.date) {
              // Reflections live worker-only; chip syncs daily entries only.
              if (val.kind && val.kind !== 'daily') continue;
              if (haveSet) {
                if (!haveSet.has(val.date)) entries.push(val);
              } else if (!maxDate || val.date > maxDate) {
                entries.push(val);
              }
            }
          }
          if (page.size < 400 || !lastKey) break;
          cursorStart = lastKey + '\x00';
        }
        // Push oldest-first so the chip writes them in chronological order.
        // Burst is fine: the chip's WS reader processes 8 frames per main
        // loop iteration, naturally throttling consumption while staying
        // responsive. Typical catch-up is 0-7 entries; even a fresh-flash
        // sync of a year of data is bounded by the chip's main loop cadence.
        entries.sort((a, b) => a.date.localeCompare(b.date));
        for (const entry of entries) {
          this._chroniclePushToChip(entry);
        }
        if (entries.length > 0) {
          const note = haveSet ? ('chip had ' + haveSet.size) : ('chip max=' + (maxDate || '∅'));
          console.log('chronicle: synced', entries.length, 'entries to chip (' + note + ')');
        }
        return;
      }
      if (msg.event === 'chronicle_sensor_retired') {
        // Chip declared a sensor permanently retired (consecutive bad-read
        // threshold crossed). Stash the event keyed by sensor name so each
        // sensor's retirement gets recorded once; subsequent duplicate
        // emissions (e.g. chip rebooted without persistent state for some
        // reason) overwrite with the latest unix but don't multiply records.
        // Chronicle UI consumes this in a later session; for now the data
        // is captured durably so nothing is lost.
        const sensor = msg.data && typeof msg.data.sensor === 'string' ? msg.data.sensor : null;
        const unix   = msg.data && Number.isFinite(msg.data.unix) ? Math.floor(msg.data.unix) : 0;
        if (sensor && /^[a-z0-9_]{2,16}$/.test(sensor)) {
          // Prefer chip-local date so chronicle UI can surface this on the
          // chip's lived day. Falls through to UTC date from unix, then
          // worker UTC today, if chip-local isn't available.
          let date = null;
          if (this.lastStats) {
            try {
              const tl = JSON.parse(this.lastStats).today_local;
              if (typeof tl === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(tl)) date = tl;
            } catch (e) {}
          }
          if (!date && unix > 0) date = new Date(unix * 1000).toISOString().slice(0, 10);
          if (!date) date = this._todayUtc();
          await this.state.storage.put('sensor_retired/' + sensor, {
            sensor, unix, date,
          });
          console.log('chronicle: sensor retired:', sensor, 'on', date);
        }
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
      if (msg.event === 'chronicle_note_set') {
        // Admin curatorial note for a Chronicle entry. Trust model is the
        // same as snake_clear: HMAC-authenticated WS from the chip's
        // admin panel. Notes only attach to existing (sealed) entries;
        // future-dated or unsealed dates are ignored silently so the
        // stash doesn't accumulate phantom records.
        //
        // RMW wrapped in blockConcurrencyWhile to serialize against
        // _chronicleMaybeSeal (also wrapped). Without this, an alarm-fired
        // seal could interleave between get() and put() and either lose
        // the note or have its own writes clobbered by the note's put.
        if (!msg.data || typeof msg.data.date !== 'string') return;
        if (!/^\d{4}-\d{2}-\d{2}$/.test(msg.data.date)) return;
        const note = typeof msg.data.note === 'string' ? msg.data.note.slice(0, 1000) : '';
        let updatedEntry = null;
        try {
          await this.state.blockConcurrencyWhile(async () => {
            const entry = await this.state.storage.get('chronicle/' + msg.data.date);
            if (!entry) return;
            if (note) entry.owner_note = note;
            else delete entry.owner_note;
            await this.state.storage.put('chronicle/' + msg.data.date, entry);
            updatedEntry = entry;
          });
        } catch (e) {
          console.error('chronicle_note_set storage failed:', e && e.message);
          return;
        }
        if (!updatedEntry) return;
        // Crosspost the note as a reply to the original tweet. No-op if
        // there's no parent tweet, no note text, or already replied.
        if (note) {
          this._chronicleMaybePostNote(msg.data.date).catch(e =>
            console.error('chronicle note reply failed:', e && e.message));
        }
        // Re-backup the entry so SD card + R2 reflect the note change.
        this._chronicleBackup(updatedEntry).catch(e =>
          console.error('chronicle backup after note failed:', e && e.message));
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

    // Chronicle: seal yesterday's snapshot when chip-local day rolls over.
    // The accumulator path normally handles this on the stats event itself;
    // this alarm-driven call is a safety net for "chip went silent right
    // after midnight before the seal could fire." Cheap unless rollover
    // actually happened (then 1 read + 2 writes).
    this._chronicleMaybeSeal().catch(() => {});

    // Daily DO storage snapshot to R2. Once-per-UTC-day catch-all so any
    // non-chronicle DO state (snake leaderboards + seasons + replays,
    // operational flags, caches) is recoverable from yesterday if DO ever
    // gets wiped. Short-circuits cheaply after the first call each day.
    this._maybeDoSnapshot().catch(e =>
      console.error('do-snapshot alarm failed:', e && e.message));

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

    // Chronicle deep-link handling. URLs like /chronicle/2026-05-04 are SPA
    // permalinks served by the chip's /chronicle handler. For social media
    // crawlers (X cards, Slack unfurl, etc.) we want per-entry OG tags so
    // each shared permalink gets a card with the actual entry's date and
    // body, not the generic Chronicle page description. Approach: if the
    // entry exists in storage, fetch the shell HTML and rewrite the meta
    // tags via HTMLRewriter. Otherwise fall through to the default rewrite
    // + relay. HTMLRewriter (vs string regex) is robust to chronicle.html's
    // meta tag formatting changing: it operates on parsed tags rather than
    // matching specific quote/whitespace patterns.
    const chronicleDateMatch = url.pathname.match(/^\/chronicle\/(\d{4}-\d{2}-\d{2}|\d{4}-W\d{2}|\d{4}-Q\d|\d{4}-\d{2})$/);
    if (chronicleDateMatch && request.method === 'GET') {
      const date = chronicleDateMatch[1];
      const entry = await this.state.storage.get('chronicle/' + date);
      if (entry) {
        try {
          const shellUrl = url.protocol + '//' + url.host + '/chronicle';
          const shellResponse = await fetch(new Request(shellUrl, {
            method: 'GET',
            headers: { 'Accept': 'text/html', 'Accept-Encoding': 'gzip' },
          }));
          if (shellResponse.ok) {
            // Title shape depends on entry kind. Daily: "Day N · May 6, 2026".
            // Weekly: "Week 18 of 2026". Monthly: "May 2026".
            let title;
            const kind = entry.kind || 'daily';
            if (kind === 'weekly') {
              const wm = /^(\d{4})-W(\d{2})$/.exec(entry.date);
              title = wm
                ? `Week ${parseInt(wm[2], 10)} of ${wm[1]} / Chronicle / HelloESP`
                : `${entry.date} / Chronicle / HelloESP`;
            } else if (kind === 'monthly') {
              const mm = /^(\d{4})-(\d{2})$/.exec(entry.date);
              if (mm) {
                const monthName = new Date(Date.UTC(+mm[1], +mm[2] - 1, 1))
                  .toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
                title = `${monthName} ${mm[1]} / Chronicle / HelloESP`;
              } else {
                title = `${entry.date} / Chronicle / HelloESP`;
              }
            } else if (kind === 'quarterly') {
              const qm = /^(\d{4})-Q(\d)$/.exec(entry.date);
              title = qm
                ? `Q${qm[2]} ${qm[1]} / Chronicle / HelloESP`
                : `${entry.date} / Chronicle / HelloESP`;
            } else {
              const dayLabel = entry.day_number ? 'Day ' + entry.day_number + ' · ' : '';
              const dateLong = new Date(entry.date + 'T12:00:00Z').toLocaleDateString('en-US', {
                year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC',
              });
              title = dayLabel + dateLong + ' / Chronicle / HelloESP';
            }
            // Description: prefer owner note + body; collapse whitespace,
            // truncate at 200 chars on word boundary so cards stay tidy.
            // Fall back to a generic line if both fields are empty so we
            // don't replace the static description with an empty string.
            let descSrc = entry.owner_note
              ? entry.owner_note + ' - ' + (entry.body || '')
              : (entry.body || '');
            descSrc = descSrc.replace(/\s+/g, ' ').trim();
            if (!descSrc) {
              descSrc = 'A daily entry the chip writes about itself.';
            }
            if (descSrc.length > 200) {
              descSrc = descSrc.substring(0, 199).replace(/\s+\S*$/, '') + '…';
            }
            const permalink = 'https://helloesp.com/chronicle/' + entry.date;

            // HTMLRewriter requires non-gzipped input. shellResponse.text()
            // auto-decompresses; wrap the resulting HTML in a fresh Response
            // so HTMLRewriter sees plain text. setAttribute / setInnerContent
            // handle attribute and HTML escaping automatically (no manual
            // escAttr needed). setInnerContent uses html:false so a stray
            // angle bracket in the title doesn't get parsed as markup.
            const html = await shellResponse.text();
            const decoded = new Response(html, {
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            });
            const transformed = new HTMLRewriter()
              .on('title', { element(el) { el.setInnerContent(title, { html: false }); } })
              .on('link[rel="canonical"]', { element(el) { el.setAttribute('href', permalink); } })
              .on('meta[name="description"]', { element(el) { el.setAttribute('content', descSrc); } })
              .on('meta[property="og:title"]', { element(el) { el.setAttribute('content', title); } })
              .on('meta[property="og:description"]', { element(el) { el.setAttribute('content', descSrc); } })
              .on('meta[property="og:url"]', { element(el) { el.setAttribute('content', permalink); } })
              .on('meta[name="twitter:title"]', { element(el) { el.setAttribute('content', title); } })
              .on('meta[name="twitter:description"]', { element(el) { el.setAttribute('content', descSrc); } })
              .transform(decoded);
            return new Response(transformed.body, {
              status: 200,
              headers: {
                'Content-Type': 'text/html; charset=utf-8',
                'Cache-Control': 'public, max-age=300',
                ...SEC_HEADERS,
              },
            });
          }
        } catch (e) {
          // Shell fetch / rewrite failed; fall through to default relay
          // path so the page still loads even if OG injection fails.
        }
      }
      // No entry, or shell fetch failed: serve the SPA shell (client-side
      // router will render the entry from chronicle.json or show 'not found').
      url.pathname = '/chronicle';
    }

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

    // ===== Chronicle endpoints =====
    // Data lives in DO storage at chronicle/YYYY-MM-DD; entries are sealed
    // by the alarm-driven _chronicleMaybeSeal at UTC midnight rollover.

    // /chronicle.json: full reverse-chronological list of sealed entries.
    // CORS-permissive so the LAN-served admin page can fetch this when
    // it's the cross-origin endpoint (admin loads the entry list to wire
    // up the per-row note editor).
    // /chronicle/preview: today's in-progress snapshot rendered through
    // the daily templates as if sealing right now. Useful for peeking at
    // what tonight's entry will look like before chip-local midnight.
    // Public, same data sensitivity as /stats (already public). Reads
    // chronicleSnapshot directly so this is "live" up to the last
    // stats_update event the chip pushed.
    if (url.pathname === '/chronicle/preview' && request.method === 'GET') {
      const snap = this.chronicleSnapshot;
      if (!snap) {
        return new Response(JSON.stringify({ error: 'no in-progress snapshot' }), {
          status: 503,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
            'Access-Control-Allow-Origin': '*',
            ...SEC_HEADERS,
          },
        });
      }
      const dayNum = this._chronicleDayNumber(snap.date);
      const ctx = { dayNumber: dayNum };
      const { template_id, body } = await this._chronicleRender(snap, ctx);
      const ageSec = this.lastStatsAt
        ? Math.max(0, Math.floor((Date.now() - this.lastStatsAt) / 1000))
        : null;
      const payload = {
        date: snap.date,
        day_number: dayNum,
        kind: 'daily',
        template_id,
        body,
        stats: this._chronicleStatsSummary(snap),
        samples: snap.samples,
        snapshot_age_seconds: ageSec,
        preview_at: Math.floor(Date.now() / 1000),
        next_seal_unix: this._nextSealUnix(),
        note: 'In-progress preview; the entry will reseal at chip-local midnight. Re-renders on every request, so a refresh shows the latest data.',
      };
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS,
        },
      });
    }

    if (url.pathname === '/chronicle.json' && request.method === 'GET') {
      // Reverse-sorted + limit 1000 keeps the response bounded past the
      // DO list cap. At year 3+ the lex-last 1000 keys (which lex-mixes
      // dailies + reflections) covers the most recent ~3 years; older
      // entries stay accessible via direct permalink. True year-aware
      // lazy loading is the next Tier 2 work when archive crosses the
      // 1000-entry threshold (see deferred memo).
      const list = await this.state.storage.list({
        prefix: 'chronicle/', reverse: true, limit: 1000,
      });
      const entries = [];
      for (const [, val] of list) {
        if (!val || typeof val !== 'object' || !val.date) continue;
        // Strip the hourly buckets from the index payload. ~600 bytes per
        // entry that the index UI never reads (only detail view uses it).
        // At year 5 saves ~1MB across ~1800 entries. Detail endpoint
        // (/chronicle/<date>.json) keeps hourly intact for time-of-day
        // detectors and any future intraday-aware UI.
        if (val.stats && val.stats.hourly) {
          const stripped = { ...val.stats };
          delete stripped.hourly;
          entries.push({ ...val, stats: stripped });
        } else {
          entries.push(val);
        }
      }
      // Attach sensor-retire data to the daily entry that matches each
      // sensor's retire date. Cheap: handful of sensor keys, single get().
      try {
        const retireList = await this.state.storage.list({ prefix: 'sensor_retired/' });
        const retiresByDate = {};
        for (const [, retire] of retireList) {
          if (retire && retire.date && retire.sensor) {
            (retiresByDate[retire.date] = retiresByDate[retire.date] || []).push({
              sensor: retire.sensor, unix: retire.unix || 0,
            });
          }
        }
        for (const e of entries) {
          if ((!e.kind || e.kind === 'daily') && retiresByDate[e.date]) {
            e.sensor_retired = retiresByDate[e.date];
          }
        }
      } catch (err) {
        console.error('sensor_retired attach failed:', err && err.message);
      }
      // Sort key: prefer period_start (set on weekly/monthly), fall back
      // to date for daily. Tie-break on kind so monthly>weekly>daily for
      // the same period start (puts the period-summary above its days).
      const KIND_RANK = { monthly: 3, weekly: 2, daily: 1 };
      const sortKey = (e) => e.period_start || e.date || '';
      entries.sort((a, b) => {
        const ka = sortKey(a), kb = sortKey(b);
        if (ka !== kb) return kb.localeCompare(ka);
        const ra = KIND_RANK[a.kind || 'daily'] || 1;
        const rb = KIND_RANK[b.kind || 'daily'] || 1;
        return rb - ra;
      });
      // Use config.launchDate for "started" rather than scanning the
      // returned entries. Past year 3 the entries list won't include the
      // actual launch entry (it's beyond the 1000-row window), but the
      // launch date itself never changes and the config has it.
      const cfg = this._chronicleConfig();
      const started = cfg.launchYear + '-' +
        String(cfg.launchMonth).padStart(2, '0') + '-' +
        String(cfg.launchDay).padStart(2, '0');
      return new Response(JSON.stringify({
        entries,
        started,
        next_seal_unix: this._nextSealUnix(),
      }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=60',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    // /chronicle/<key>.json: single entry detail with prev/next neighbor
    // keys. <key> can be daily (YYYY-MM-DD), weekly (YYYY-WNN), or
    // monthly (YYYY-MM). Neighbors stay within the same kind so prev/next
    // navigation reads as "previous week / next month", not jumping
    // across granularities.
    {
      const m = url.pathname.match(/^\/chronicle\/(\d{4}-\d{2}-\d{2}|\d{4}-W\d{2}|\d{4}-Q\d|\d{4}-\d{2})\.json$/);
      if (m && request.method === 'GET') {
        const date = m[1];
        const entry = await this.state.storage.get('chronicle/' + date);
        if (!entry) {
          return new Response(JSON.stringify({ error: 'no entry for that key' }), {
            status: 404,
            headers: {
              'Content-Type': 'application/json',
              'Cache-Control': 'no-store',
              'Access-Control-Allow-Origin': '*',
              ...SEC_HEADERS
            }
          });
        }
        const requestedKind = (entry.kind || 'daily');
        const isDaily     = (k) => /^\d{4}-\d{2}-\d{2}$/.test(k);
        const isWeekly    = (k) => /^\d{4}-W\d{2}$/.test(k);
        const isQuarterly = (k) => /^\d{4}-Q\d$/.test(k);
        const isMonthly   = (k) => /^\d{4}-\d{2}$/.test(k);
        const matchKind = requestedKind === 'weekly' ? isWeekly
                        : requestedKind === 'quarterly' ? isQuarterly
                        : requestedKind === 'monthly' ? isMonthly
                        : isDaily;
        // Attach sensor-retire data if this is a daily entry that matches
        // a sensor's retire date.
        if (requestedKind === 'daily') {
          try {
            const retireList = await this.state.storage.list({ prefix: 'sensor_retired/' });
            const retires = [];
            for (const [, retire] of retireList) {
              if (retire && retire.date === date && retire.sensor) {
                retires.push({ sensor: retire.sensor, unix: retire.unix || 0 });
              }
            }
            if (retires.length) entry.sensor_retired = retires;
          } catch (err) {
            console.error('sensor_retired attach failed:', err && err.message);
          }
        }
        // Bounded prev + next lookups via cursor pagination so navigation
        // works correctly past the DO 1000-entry list cap. Each direction
        // walks until it hits a same-kind neighbor or the end of the list.
        // Typical hit costs one page (~10ms) since neighbors are usually
        // 1-2 keys away after sort interleaves kinds.
        const findNeighbor = async (direction) => {
          const isReverse = direction === 'prev';
          let cursor = isReverse
            ? 'chronicle/' + date
            : 'chronicle/' + date + '\x00';
          while (true) {
            const opts = { prefix: 'chronicle/', limit: 200 };
            if (isReverse) { opts.end = cursor; opts.reverse = true; }
            else { opts.start = cursor; }
            const page = await this.state.storage.list(opts);
            if (page.size === 0) return null;
            let lastKey = null;
            for (const [k] of page) {
              lastKey = k;
              const d = k.replace('chronicle/', '');
              if (matchKind(d) && d !== date) return d;
            }
            if (page.size < 200 || !lastKey) return null;
            cursor = isReverse ? lastKey : lastKey + '\x00';
          }
        };
        const [prev, next] = await Promise.all([findNeighbor('prev'), findNeighbor('next')]);
        return new Response(JSON.stringify({ entry, prev, next }), {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=300',
            'Access-Control-Allow-Origin': '*',
            ...SEC_HEADERS
          }
        });
      }
    }

    // /chronicle.rss: feed of recent entries for subscribers. Keeps the
    // last 30 sealed entries so the feed stays small while still covering
    // a month of activity. rfc822 falls back to the entry date at noon UTC
    // when sealed_at is missing or zero (defensive; shouldn't happen in
    // current code paths but cheap to handle). Reflections are filtered
    // out (RSS reads as a daily-cadence feed; weekly/monthly summaries
    // would mix with dailies awkwardly under date-string sort).
    if (url.pathname === '/chronicle.rss' && request.method === 'GET') {
      // reverse+limit so this works correctly past the DO 1000-entry list
      // cap. Over-fetch (limit 200) to compensate for the daily filter
      // dropping ~10% of returned keys (interleaved reflections).
      const list = await this.state.storage.list({
        prefix: 'chronicle/', reverse: true, limit: 200,
      });
      const entries = [];
      for (const [, val] of list) {
        if (val && typeof val === 'object' && val.date) {
          if (val.kind && val.kind !== 'daily') continue;
          entries.push(val);
          if (entries.length >= 30) break;
        }
      }
      const recent = entries;
      const escapeXml = (s) => String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
      const rfc822 = (entry) => {
        const t = entry.sealed_at && entry.sealed_at > 0
          ? new Date(entry.sealed_at * 1000)
          : new Date(entry.date + 'T12:00:00Z');
        return t.toUTCString();
      };
      let rss = '<?xml version="1.0" encoding="UTF-8"?>\n'
              + '<rss version="2.0"><channel>'
              + '<title>HelloESP | Chronicle</title>'
              + '<link>https://helloesp.com/chronicle</link>'
              + '<description>A daily entry the chip writes about itself.</description>'
              + '<language>en</language>';
      for (const e of recent) {
        const title = `Day ${e.day_number || '?'} · ${e.date}`;
        const link = `https://helloesp.com/chronicle/${e.date}`;
        let desc = e.body || '';
        if (e.owner_note) desc = `${e.owner_note}\n\n${desc}`;
        rss += '<item>'
             + `<title>${escapeXml(title)}</title>`
             + `<link>${escapeXml(link)}</link>`
             + `<guid isPermaLink="false">chronicle-${e.date}</guid>`
             + `<pubDate>${rfc822(e)}</pubDate>`
             + `<description>${escapeXml(desc)}</description>`
             + '</item>';
      }
      rss += '</channel></rss>';
      return new Response(rss, {
        status: 200,
        headers: {
          'Content-Type': 'application/rss+xml; charset=utf-8',
          'Cache-Control': 'public, max-age=300',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    // /chronicle/preview: render today's in-progress snapshot via templates
    // WITHOUT sealing or persisting. Useful for previewing what the entry
    // for today would look like before midnight rolls over. Returns 404
    // until the first stats sample lands so previews don't lie about days
    // the chip wasn't awake.
    if (url.pathname === '/chronicle/preview' && request.method === 'GET') {
      const snap = this.chronicleSnapshot;
      if (!snap || snap.samples === 0) {
        return new Response(JSON.stringify({ error: 'no samples yet today' }), {
          status: 404,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
            'Access-Control-Allow-Origin': '*',
            ...SEC_HEADERS
          }
        });
      }
      const dayNum = this._chronicleDayNumber(snap.date);
      const { template_id, body } = this._chronicleRender(snap, { dayNumber: dayNum });
      return new Response(JSON.stringify({
        preview: true,
        date: snap.date,
        day_number: dayNum,
        template_id,
        body,
        stats: this._chronicleStatsSummary(snap),
      }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'Access-Control-Allow-Origin': '*',
          ...SEC_HEADERS
        }
      });
    }

    // DO Storage Explorer: read-only inspection plus delete-by-key for the
    // worker's Durable Object storage. Used by the admin panel's DO Explorer
    // section. Auth via Authorization: Bearer WORKER_SECRET header so the
    // secret never appears in URLs (history, referrer, server logs). Same
    // trust model as other admin tooling: LAN admin page reaches across
    // origin to the worker, operator pastes secret once per browser session.
    // Per-IP rate limit prevents a leaked secret from being used for bulk
    // scraping. No write/put endpoint, only read + delete: editing arbitrary
    // DO state from a UI is too easy to misuse and not needed for debugging.
    if (url.pathname.startsWith('/admin/do/')) {
      const corsHdrs = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Authorization',
        'Access-Control-Max-Age': '600',
      };
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: corsHdrs });
      }
      const authHeader = request.headers.get('Authorization') || '';
      const bm = authHeader.match(/^Bearer\s+(.+)$/);
      const providedKey = bm ? bm[1] : '';
      if (!providedKey || !timingSafeEqualStr(providedKey, this.env.WORKER_SECRET || '')) {
        return new Response('Unauthorized', { status: 401, headers: corsHdrs });
      }
      // Per-IP rate limit. 30/min for DO ops; mirror /guestbook/translate
      // pattern. Bulk scraping a 1000-key DO would still take ~30 minutes
      // even with a leaked secret, giving time to rotate.
      const dIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const dNow = Date.now();
      let drl = this.rateLimits.get('doadmin:' + dIP);
      if (!drl || dNow > drl.resetAt) {
        drl = { count: 0, resetAt: dNow + 60000 };
        this.rateLimits.set('doadmin:' + dIP, drl);
      }
      drl.count++;
      if (drl.count > 30) {
        return new Response('Rate limit exceeded; try again in a minute',
          { status: 429, headers: corsHdrs });
      }
      const jsonHdrs = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...corsHdrs };
      if (url.pathname === '/admin/do/list' && request.method === 'GET') {
        const prefix = url.searchParams.get('prefix') || '';
        const limit = Math.min(1000, Math.max(1, parseInt(url.searchParams.get('limit') || '500', 10)));
        if (prefix.length > 100) return new Response('Bad prefix', { status: 400, headers: corsHdrs });
        const opts = { limit };
        if (prefix) opts.prefix = prefix;
        const list = await this.state.storage.list(opts);
        const items = [];
        for (const [k, v] of list) {
          let size = 0;
          try { size = JSON.stringify(v).length; } catch (e) {}
          items.push({ key: k, size });
        }
        return new Response(JSON.stringify({ items, count: items.length }), { status: 200, headers: jsonHdrs });
      }
      if (url.pathname === '/admin/do/get' && request.method === 'GET') {
        const k = url.searchParams.get('k') || '';
        if (!k || k.length > 200) return new Response('Bad key', { status: 400, headers: corsHdrs });
        const val = await this.state.storage.get(k);
        if (val === undefined) return new Response('Key not found', { status: 404, headers: corsHdrs });
        return new Response(JSON.stringify({ key: k, value: val }), { status: 200, headers: jsonHdrs });
      }
      if (url.pathname === '/admin/do/delete' && request.method === 'DELETE') {
        const k = url.searchParams.get('k') || '';
        if (!k || k.length > 200) return new Response('Bad key', { status: 400, headers: corsHdrs });
        const existed = await this.state.storage.delete(k);
        console.log('do/delete:', k, 'existed=', existed);
        return new Response(JSON.stringify({ deleted: k, existed: !!existed }), { status: 200, headers: jsonHdrs });
      }
      return new Response('Not found', { status: 404, headers: corsHdrs });
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

      const targetLang = target.split('-')[0];
      // Helper: single m2m100 attempt. Returns the cleaned translated string
      // or '' on any failure (network error, empty model output, model
      // echoing the input, etc.). The `<>`-strip is a defensive cleanup
      // since frontends use textContent and shouldn't render HTML, but a
      // model that echoes injected markup shouldn't get a chance.
      const tryTranslate = async (src) => {
        try {
          const resp = await this.env.AI.run('@cf/meta/m2m100-1.2b', {
            text,
            source_lang: src,
            target_lang: targetLang,
          });
          const out = (resp && typeof resp.translated_text === 'string'
            ? resp.translated_text : '').replace(/[<>]/g, '').trim();
          return out;
        } catch (e) {
          console.error('translate AI.run failed src=' + src + ':', e && e.message);
          return '';
        }
      };
      // Helper: identify source language via a small instruct LLM. Used
      // when the client sends source 'auto' (heuristics couldn't confidently
      // identify the source). llama-3.2-1b is plenty for "what language is
      // this" since the answer is a 2-character ISO code; max_tokens kept
      // tight to avoid hallucinated explanations. Returns null on any
      // failure (model error, malformed output, ambiguous text). Cost:
      // single AI call, ~500ms typical. Output validation extracts the
      // first 2-letter token to tolerate occasional padding or punctuation.
      const detectLanguage = async (txt) => {
        try {
          const resp = await this.env.AI.run('@cf/meta/llama-3.2-1b-instruct', {
            messages: [
              { role: 'system', content: 'You identify languages. Respond with only a 2-letter lowercase ISO 639-1 language code (en, cs, pl, de, fr, it, es, ru, zh, ja, ar, etc.), nothing else. No punctuation, no explanation.' },
              { role: 'user', content: 'What language is this text? ' + txt.slice(0, 400) },
            ],
            max_tokens: 8,
            temperature: 0,
          });
          const out = (resp && typeof resp.response === 'string' ? resp.response : '').trim().toLowerCase();
          const m = out.match(/\b([a-z]{2})\b/);
          return m ? m[1] : null;
        } catch (e) {
          console.error('language detect failed:', e && e.message);
          return null;
        }
      };
      // Last-resort: have the LLM translate directly. m2m100 expects
      // properly-accented input; diacritic-less text (e.g., "Plzen zdravi
      // sveho bratrance" instead of "Plzeň zdraví svého bratrance") often
      // produces empty/unchanged output. llama-3.2-1b's multilingual
      // training handles unaccented variants more gracefully. Quality is
      // lower than purpose-built translation models but reliably non-empty.
      const translateViaLLM = async (txt, tgtLang) => {
        const langName = ({
          en:'English', cs:'Czech', pl:'Polish', de:'German', fr:'French',
          it:'Italian', es:'Spanish', ru:'Russian', zh:'Chinese',
          ja:'Japanese', ko:'Korean', ar:'Arabic', pt:'Portuguese',
          nl:'Dutch', tr:'Turkish', uk:'Ukrainian', he:'Hebrew', hi:'Hindi',
          th:'Thai', el:'Greek', sk:'Slovak', hu:'Hungarian', ro:'Romanian',
          sv:'Swedish', no:'Norwegian', da:'Danish', fi:'Finnish',
        })[tgtLang] || tgtLang;
        try {
          const resp = await this.env.AI.run('@cf/meta/llama-3.2-1b-instruct', {
            messages: [
              { role: 'system', content: 'You translate text. Respond with only the translation. No explanation, no labels, no quotes around the output.' },
              { role: 'user', content: 'Translate this to ' + langName + ':\n\n' + txt.slice(0, 1000) },
            ],
            max_tokens: 400,
            temperature: 0.1,
          });
          const out = (resp && typeof resp.response === 'string' ? resp.response : '').replace(/[<>]/g, '').trim();
          return out;
        } catch (e) {
          console.error('LLM translate failed:', e && e.message);
          return '';
        }
      };
      let translated = '';
      if (source && source !== 'auto') {
        // Client supplied a confident guess (guessLatinLang result or a
        // non-Latin script default). Trust it; one m2m100 call.
        translated = await tryTranslate(source);
      } else {
        // Source unknown. LLM identifies, then m2m100 translates. Total:
        // 2 AI calls per first-time translation, both cached after.
        const detected = await detectLanguage(text);
        if (detected && detected !== targetLang) {
          translated = await tryTranslate(detected);
        }
        // Fallback chain if LLM detection failed entirely OR m2m100
        // rejected the detected source.
        if (!translated) {
          const candidates = ['cs', 'pl', 'de'];
          const inputLower = text.toLowerCase();
          for (const candidate of candidates) {
            if (candidate === targetLang) continue;
            const result = await tryTranslate(candidate);
            if (result && result.toLowerCase() !== inputLower) {
              translated = result;
              break;
            }
          }
        }
      }
      // Final fallback: m2m100 produced nothing useful through any path.
      // Most often this is diacritic-less text (Czech/Polish/Slovak typed
      // without accents) that m2m100 can't handle. Ask the LLM to translate
      // directly; output quality is lower but reliably non-empty.
      if (!translated) {
        translated = await translateViaLLM(text, targetLang);
        // Guard against the LLM echoing the input verbatim or returning
        // a refusal like "I cannot translate this." A simple identity
        // check catches echoes; refusals are accepted as-is since they
        // at least give the user something.
        if (translated && translated.toLowerCase() === text.toLowerCase()) {
          translated = '';
        }
      }
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
