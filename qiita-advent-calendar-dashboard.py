import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime, date
import cdata.connect as mod
import streamlit_shadcn_ui as ui

@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_analytics_data():
    """
    Connect Cloud からGA4のデータを取得して、以下のリストのリストを返します。
    - 日付
    - ブログ記事のURL
    - アクセスしたユーザー数
    例: [['/jp/blog/a-post', 1], ['/jp/blog/another-post', 5]]
    """
    try:
        conn = mod.connect("AuthScheme=OAuth;")

        cur = conn.execute("""
            SELECT Date, PagePath, TotalUsers
            FROM [DATASOURCE].[GoogleAnalytics4].[EngagementPagesPathReport]
            WHERE PagePath LIKE '%/blog/%'
            AND StartDate = '2024-12-01'
            AND EndDate = '2024-12-31'
        """)
        result = [[row[0], row[1], row[2]] for row in cur.fetchall()]

        return result
    except Exception as e:
        st.error(f"アナリティクスデータの取得中にエラーが発生しました: {str(e)}")
        return []  # エラー時は空のリストを返す

def get_calendar_data(analytics_data=None):
    try:
        url = "CALENDAR_URL"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # まず、アナリティクスデータなしでカレンダーエントリを収集
        calendar_items = []
        seen_urls = set()
        seen_author_title_pairs = set()

        daily_visitors = []

        # すべてのカレンダーエントリを検索
        calendar_entries = soup.find_all('div', class_='style-176zglo')

        for entry in calendar_entries:
            # 著者を取得
            author_link = entry.find('a', class_='style-zfknvc')
            author = author_link.text.strip().replace('@', '') if author_link else ""

            # タイトルを取得
            title_div = entry.find('div', class_='style-mpez5z')
            title = title_div.text.strip() if title_div else ""

            # URLを取得
            url = ""
            if title_div and title_div.find('a'):
                url = title_div.find('a').get('href', '')

            # 著者、タイトル、URLがない場合はスキップ
            if not (author or title or url):
                continue

            # 重複チェック
            is_duplicate = False

            # URLが存在する場合、URL重複をチェック
            if url:
                if url in seen_urls:
                    is_duplicate = True
                else:
                    seen_urls.add(url)

            # 著者とタイトルが存在する場合、著者-タイトルの重複をチェック
            if author and title:
                author_title_pair = (author, title)
                if author_title_pair in seen_author_title_pairs:
                    is_duplicate = True
                else:
                    seen_author_title_pairs.add(author_title_pair)

            # 重複していないエントリを追加
            if not is_duplicate:
                calendar_items.append({
                    'author': author,
                    'title': title,
                    'url': url,
                    'visitors': 0
                })


        # アナリティクスデータが利用可能な場合、処理を行う
        if analytics_data is not None:
            valid_urls = {item['url'] for item in calendar_items if item['url']}

            # URL基準と日付基準の集計用に辞書を作成
            url_visitors = {}
            date_visitors = {}

            # フィルタリング用の開始日を定義（12月1日）
            start_date = date(2024, 12, 1)

            # GA4データを処理
            for date_obj, path, count in analytics_data:
                # 12月1日より前の日付はスキップ
                if date_obj < start_date:
                    continue

                full_url = f"https://www.cdata.com{path}"

                # URLがカレンダーアイテムに存在する場合のみ処理
                if full_url in valid_urls:
                    # URL別に集計
                    if full_url in url_visitors:
                        url_visitors[full_url] += count
                    else:
                        url_visitors[full_url] = count

                    # 日付をフォーマットし、日付別に集計
                    formatted_date = date_obj.strftime('%Y年%m月%d日').replace('0日', '日')
                    if formatted_date in date_visitors:
                        date_visitors[formatted_date] += count
                    else:
                        date_visitors[formatted_date] = count

            # カレンダーアイテムの訪問者数を更新
            for item in calendar_items:
                if item['url'] in url_visitors:
                    item['visitors'] = url_visitors[item['url']]

            # 日別訪問者リストを作成、元の日付順にソート
            daily_visitors = [
                {
                    'date': formatted_date,
                    'visitors': count
                }
                for formatted_date, count in sorted(date_visitors.items(),
                    key=lambda x: datetime.strptime(x[0], '%Y年%m月%d日'))
            ]

        return calendar_items, daily_visitors

    except Exception as e:
        st.error(f"データの取得中にエラーが発生しました: {str(e)}")
        return [], []

def main():
    # ページ設定
    st.set_page_config(
        page_title="CData Advent Calendar 2024",
        page_icon="📅"
    )

    # データベースに接続
    try:
        analytics_data = get_analytics_data()
    except Exception as e:
        st.error(f"データベース接続中にエラーが発生しました: {str(e)}")
        analytics_data = {}

    # ヘッダー
    st.header("🎄CData Software Advent Calendar 2024🎄")

    # データを取得
    calendar_items, daily_visitors = get_calendar_data(analytics_data)

    if calendar_items:
        # DataFrameを作成
        df = pd.DataFrame(calendar_items)
        daily_df = pd.DataFrame(daily_visitors)

        # 予約済み記事と投稿済みの記事数
        filled_slots = len(df[(df['author'] != "") & (df['title'] != "")])
        posted_articles = len(df[df['url'] != ""])

        st.subheader("投稿済み記事数")
        progress_value = float(posted_articles)/float(filled_slots) if filled_slots > 0 else 0.0
        st.progress(progress_value, text=f"{posted_articles}/{filled_slots}")

        # 日別訪問者チャートを追加
        st.subheader("日別閲覧数")
        if not daily_df.empty:
            daily_df['date'] = daily_df['date'].astype(str)

            # 棒グラフ用に日付をインデックスとして設定
            daily_df = daily_df.set_index('date')

            # Streamlitを使用して棒グラフを作成
            st.bar_chart(
                daily_df['visitors'],
                height=400
            )


        # フィルターを追加
        st.subheader("著者で絞り込み")
        author_filter = st.multiselect(
            "著者",
            options=sorted(df[df['author'] != '']['author'].unique())
        )

        # フィルターを適用してテーブルを表示
        filtered_df = df.copy()
        if author_filter:
            filtered_df = filtered_df[filtered_df['author'].isin(author_filter)]

        st.subheader("記事一覧")
        st.dataframe(
            filtered_df,
            column_config={
                "visitors": st.column_config.NumberColumn("閲覧ユーザー数", width="small", format="%d"),
                "author": st.column_config.TextColumn("著者", width="medium"),
                "title": st.column_config.TextColumn("記事タイトル", width="large"),
                "url": st.column_config.LinkColumn("URL", width="medium")
            },
            hide_index=True
        )

        st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    else:
        st.error("データを取得できませんでした。後でもう一度お試しください。")

if __name__ == "__main__":
    main()
