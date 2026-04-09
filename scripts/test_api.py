import requests, re, json

addr = "0xc579d0db0d1b52635c87315defd141d28384e386"
r = requests.get(f"https://polymarket.com/profile/{addr}",
    headers={"User-Agent": "Mozilla/5.0"}, timeout=15)

print(f"Full page: {len(r.text):,} bytes")

build_id = re.search(r'"buildId":"([^"]+)"', r.text)
if build_id:
    bid = build_id.group(1)
    print(f"Build ID: {bid}")
    data_url = f"https://polymarket.com/_next/data/{bid}/profile/{addr}.json"
    r2 = requests.get(data_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    print(f"Next.js data: {r2.status_code}, {len(r2.text):,} bytes")
    if r2.status_code == 200:
        d = r2.json()
        props = d.get("pageProps", {})
        print(f"Keys: {list(props.keys())[:10]}")
        if "user" in props:
            u = props["user"]
            print(f"Username: {u.get('username')}")
            print(f"Bio: {u.get('bio', '')[:100]}")
