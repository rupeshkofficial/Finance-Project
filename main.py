"""
multi_index_oi_tracker.py

Author: Rupesh Kr
Created: May 2025

"""

import os, json, urllib.request, threading, datetime as dt, time
import pandas as pd
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from collections import deque
import ssl, certifi

class MultiIndexOITracker:
    _instance = None

    @staticmethod
    def get_instance(jwt, feed_token, api_key, username):
        if MultiIndexOITracker._instance is None:
            MultiIndexOITracker(jwt, feed_token, api_key, username)
        return MultiIndexOITracker._instance

    def __init__(self, jwt, feed_token, api_key, username):
        if MultiIndexOITracker._instance is not None:
            raise Exception("This is a singleton class!")
        MultiIndexOITracker._instance = self

        self.jwt = jwt
        self.feed_token = feed_token
        self.api_key = api_key
        self.username = username

        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

        self.indices = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

        self.option_data = {index: {} for index in self.indices}
        self.oi_history = {index: deque(maxlen=1440) for index in self.indices}
        self.current_oi = {
            index: {'ce_oi': 0, 'pe_oi': 0, 'ce_volume': 0, 'pe_volume': 0, 'timestamp': None}
            for index in self.indices
        }

        self.time_windows = {
            'Last 5 mins': 5, 'Last 10 mins': 10,
            'Last 15 mins': 15, 'Last 30 mins': 30,
            'Last 1 Hr': 60, 'Last 2 Hrs': 120,
            'Last 3 Hrs': 180, 'Full Day': 1440
        }

        self.token_to_symbol = {}
        self.token_to_index = {}
        self.all_tokens = []
        self.df_options = {}

        self.instruments = self._load_instruments()
        self._init_tokens()
        self._start_websocket()
        self._start_updater_thread()

    def _load_instruments(self):
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        try:
            response = urllib.request.urlopen(url, context=self.ssl_context)
            return json.loads(response.read())
        except:
            import requests
            response = requests.get(url, verify=certifi.where())
            return response.json()

    def _init_tokens(self):
        for index in self.indices:
            self.df_options[index] = self._filter_index_options(self.instruments, index)
            for _, row in self.df_options[index].iterrows():
                token = row['token']
                symbol = row['symbol']
                self.token_to_symbol[token] = symbol
                self.token_to_index[token] = index

            ce_tokens = self.df_options[index][self.df_options[index]['symbol'].str.endswith('CE')]['token'].tolist()
            pe_tokens = self.df_options[index][self.df_options[index]['symbol'].str.endswith('PE')]['token'].tolist()
            self.all_tokens.extend(ce_tokens + pe_tokens)

    def _filter_index_options(self, instruments, index_name):
        options = [i for i in instruments if i['name'] == index_name and i['instrumenttype'] == 'OPTIDX']
        if not options:
            print(f"No options found for {index_name}")
            return pd.DataFrame()

        df = pd.DataFrame(options)
        try:
            df["expiry"] = pd.to_datetime(df["expiry"], format='%d%b%Y', errors='coerce')
            nearest_expiry = df["expiry"].min()
            return df[df["expiry"] == nearest_expiry].reset_index(drop=True)
        except Exception as e:
            print(f"Error processing {index_name} options: {str(e)}")
            return pd.DataFrame()

    def _start_websocket(self):
        self.sws = SmartWebSocketV2(self.jwt, self.api_key, self.username, self.feed_token)
        self.sws.on_data = self._on_data
        self.sws.on_open = self._on_open
        self.sws.on_error = lambda ws, err: print("WebSocket error:", err)
        self.sws.on_close = lambda ws, code, msg: print("WebSocket closed")
        threading.Thread(target=self.sws.connect, daemon=True).start()

    def _on_open(self, wsapp):
        if self.all_tokens:
            token_list = [{"exchangeType": 2, "tokens": self.all_tokens}]
            self.sws.subscribe("oi_stream", 3, token_list)
            print(f"Subscribed to WebSocket for {len(self.all_tokens)} options")
        else:
            print("No tokens to subscribe to")

    def _on_data(self, wsapp, message):
        token = message['token']
        symbol = self.token_to_symbol.get(token, '')
        index = self.token_to_index.get(token, '')
        if not symbol or not index:
            return

        self.option_data[index][token] = {
            'symbol': symbol,
            'open_interest': message.get('open_interest', 0),
            'volume': message.get('last_traded_quantity', 0),
            'timestamp': dt.datetime.now()
        }

    def _start_updater_thread(self):
        def update_loop():
            while True:
                for index in self.indices:
                    if index in self.df_options and not self.df_options[index].empty and self.option_data[index]:
                        try:
                            self._update_current_data(index)
                            self.oi_history[index].append(self.current_oi[index].copy())
                        except Exception as e:
                            print(f"Error updating data for {index}: {str(e)}")
                time.sleep(60)

        threading.Thread(target=update_loop, daemon=True).start()

    def _update_current_data(self, index):
        ce_oi = sum(data.get('open_interest', 0) for data in self.option_data[index].values() if 'CE' in data.get('symbol', ''))
        pe_oi = sum(data.get('open_interest', 0) for data in self.option_data[index].values() if 'PE' in data.get('symbol', ''))
        ce_volume = sum(data.get('volume', 0) for data in self.option_data[index].values() if 'CE' in data.get('symbol', ''))
        pe_volume = sum(data.get('volume', 0) for data in self.option_data[index].values() if 'PE' in data.get('symbol', ''))

        self.current_oi[index] = {
            'ce_oi': ce_oi,
            'pe_oi': pe_oi,
            'ce_volume': ce_volume,
            'pe_volume': pe_volume,
            'timestamp': dt.datetime.now()
        }

    def get_current_oi(self, index):
        return self.current_oi[index]

    def get_oi_history_df(self, index):
        df_list = [
            {
                'Timestamp': rec['timestamp'],
                'CE_OI': rec['ce_oi'],
                'PE_OI': rec['pe_oi'],
                'CE_Volume': rec.get('ce_volume', 0),
                'PE_Volume': rec.get('pe_volume', 0)
            }
            for rec in self.oi_history[index]
        ]
        return pd.DataFrame(df_list)

    def get_analysis_table(self, index):
        data = []
        for label, minutes in self.time_windows.items():
            result = self._get_data_change_and_ratios(index, minutes)
            if result[0] is not None:
                ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change, oi_ratio, vol_ratio = result
                trend, signal = self.determine_market_trend_for_buyers(ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change)
                data.append({
                    "Time Window": label,
                    "Call OI Change": ce_oi_change,
                    "Put OI Change": pe_oi_change,
                    "Call Vol Change": ce_vol_change,
                    "Put Vol Change": pe_vol_change,
                    "CE/PE OI Ratio": round(oi_ratio, 2),
                    "CE/PE Vol Ratio": round(vol_ratio, 2) if vol_ratio else 0,
                    "Trend": trend,
                    "Signal": signal
                })
        return pd.DataFrame(data)

    def _get_data_change_and_ratios(self, index, minutes_back):
        if not self.oi_history[index]:
            return None, None, None, None, None, None

        target_time = dt.datetime.now() - dt.timedelta(minutes=minutes_back)
        closest = min(self.oi_history[index], key=lambda r: abs((r['timestamp'] - target_time).total_seconds()))
        ce_oi_change = self.current_oi[index]['ce_oi'] - closest['ce_oi']
        pe_oi_change = self.current_oi[index]['pe_oi'] - closest['pe_oi']
        ce_vol_change = self.current_oi[index]['ce_volume'] - closest.get('ce_volume', 0)
        pe_vol_change = self.current_oi[index]['pe_volume'] - closest.get('pe_volume', 0)
        oi_ratio = self.current_oi[index]['ce_oi'] / self.current_oi[index]['pe_oi'] if self.current_oi[index]['pe_oi'] else 0
        vol_ratio = self.current_oi[index]['ce_volume'] / self.current_oi[index]['pe_volume'] if self.current_oi[index]['pe_volume'] else 0
        return ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change, oi_ratio, vol_ratio

    def determine_market_trend_for_buyers(self, ce_oi_change, pe_oi_change, ce_vol_change=0, pe_vol_change=0, oi_ratio=None, vol_ratio=None):
        oi_threshold = 50000
        strong_threshold = 100000

        if abs(ce_oi_change) < oi_threshold and abs(pe_oi_change) < oi_threshold:
            return "No Trade (Sideways)", "NEUTRAL"

        ce_signal = "NEUTRAL"
        if ce_oi_change > strong_threshold:
            ce_signal = "STRONG BEARISH"
        elif ce_oi_change > oi_threshold:
            ce_signal = "BEARISH"
        elif ce_oi_change < -strong_threshold:
            ce_signal = "STRONG BULLISH"
        elif ce_oi_change < -oi_threshold:
            ce_signal = "BULLISH"

        pe_signal = "NEUTRAL"
        if pe_oi_change > strong_threshold:
            pe_signal = "STRONG BULLISH"
        elif pe_oi_change > oi_threshold:
            pe_signal = "BULLISH"
        elif pe_oi_change < -strong_threshold:
            pe_signal = "STRONG BEARISH"
        elif pe_oi_change < -oi_threshold:
            pe_signal = "BEARISH"

        volume_confirms = (
            ("BEARISH" in ce_signal and ce_vol_change > 0) or
            ("BEARISH" in pe_signal and pe_vol_change < 0) or
            ("BULLISH" in ce_signal and ce_vol_change < 0) or
            ("BULLISH" in pe_signal and pe_vol_change > 0)
        )

        final_signal = "NEUTRAL"
        if "BULLISH" in ce_signal and "BULLISH" in pe_signal:
            final_signal = "STRONG BULLISH" if "STRONG" in ce_signal or "STRONG" in pe_signal else "BULLISH"
        elif "BEARISH" in ce_signal and "BEARISH" in pe_signal:
            final_signal = "STRONG BEARISH" if "STRONG" in ce_signal or "STRONG" in pe_signal else "BEARISH"
        elif "STRONG" in ce_signal:
            final_signal = ce_signal
        elif "STRONG" in pe_signal:
            final_signal = pe_signal
        elif ce_signal != "NEUTRAL":
            final_signal = ce_signal
        elif pe_signal != "NEUTRAL":
            final_signal = pe_signal

        if volume_confirms and not final_signal.startswith("STRONG") and final_signal != "NEUTRAL":
            final_signal = "STRONG " + final_signal

        trend_map = {
            "STRONG BULLISH": "Strong Bullish (Buy CE Options)",
            "BULLISH": "Bullish (Buy CE Options)",
            "STRONG BEARISH": "Strong Bearish (Buy PE Options)",
            "BEARISH": "Bearish (Buy PE Options)"
        }
        trend = trend_map.get(final_signal, "Sideways (No Clear Signal)")
        return trend, final_signal

    def save_to_csv(self, index=None):
        indices_to_export = [index] if index else self.indices
        for idx in indices_to_export:
            if self.oi_history[idx]:
                df = self.get_oi_history_df(idx)
                df["CE_PE_OI_Ratio"] = df["CE_OI"] / df["PE_OI"].replace(0, 1)
                df["CE_PE_Volume_Ratio"] = df["CE_Volume"] / df["PE_Volume"].replace(0, 1)
                df.to_csv(f"{idx.lower()}_oi_vol_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
