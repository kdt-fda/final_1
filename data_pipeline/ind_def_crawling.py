import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

VIEW_URL = "https://kssc.mods.go.kr:8443/ksscNew_web/kssc/common/ClassificationContentMainTreeListView.do"
MAIN_URL = "https://kssc.mods.go.kr:8443/ksscNew_web/kssc/common/ClassificationContent.do?gubun=1&strCategoryNameCode=001&categoryMenu=007&addGubun=no"


def fetch_ind_page(session, ind_code):
    data = {
        "strCategoryNameCode": "001",
        "strCategoryCode": str(ind_code).zfill(2),
        "strCategoryDegree": "11",
        "categoryMenu": "007",
        "strGubun": "0",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": MAIN_URL,
        "Origin": "https://kssc.mods.go.kr:8443",
        "X-Requested-With": "XMLHttpRequest",
        "X-Prototype-Version": "1.6.0.3",
    }

    res = session.post(VIEW_URL, data=data, headers=headers, timeout=20)
    res.raise_for_status()
    return res.text


def extract_ind_def(html):
    soup = BeautifulSoup(html, "html.parser")

    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")

        if th and td and th.get_text(strip=True) == "설명":
            return td.get_text(" ", strip=True)

    return None


def main():
    df = pd.read_csv("..\\data\\ind_basic.csv", dtype={"ind_code": str})

    session = requests.Session()
    session.get(
        MAIN_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20
    )

    ind_defs = []

    for ind_code in df["ind_code"]:
        code = str(ind_code).zfill(2)
        print(f"processing {code} ...")

        try:
            html = fetch_ind_page(session, code)
            ind_def = extract_ind_def(html)
            print(f"ind_def[{code}] = {ind_def}")
            ind_defs.append(ind_def)

        except Exception as e:
            print(f"failed {code}: {e}")
            ind_defs.append(None)

        time.sleep(0.5)

    df["ind_def"] = ind_defs
    df.to_csv("..\\data\\ind_basic_filled.csv", index=False, encoding="utf-8-sig")
    print("saved: ..\\data\\ind_basic_filled.csv")


if __name__ == "__main__":
    main()