import yfinance as yf
import pandas as pd
import numpy as np

class PreProcess:
    def __init__(self):
        self.symbol_usdt_to_brl = 'BRL=X'
        self.symbol_cmin3 = 'CMIN3.SA'
        self.start_date = '2021-02-22'
        self.end_date = '2024-11-20'
        self.start_date_test_model = '2024-11-21'
        self.path_files_original = "originalFiles"
        self.path_files_process = "preprocessFiles"


    def download_and_save_files(self):
        df_usdt_to_brl_origin = yf.download(self.symbol_usdt_to_brl, start=self.start_date, end=self.end_date)
        df_cmin3_origin = yf.download(self.symbol_cmin3, start=self.start_date, end=self.end_date)
        df_cmin3_origin_test_model = yf.download(self.symbol_cmin3, start=self.start_date_test_model)
        df_usdt_to_brl_origin.to_csv(self.path_files_original + "/usdt2brl.csv")
        df_cmin3_origin.to_csv(self.path_files_original + "/cmin3.csv")
        df_cmin3_origin_test_model.to_csv(self.path_files_original + "/cmin3_test_model.csv")
        
    def ajusting_tables_TIOc1(self,df):
        df_a = df
        try:
            df_a['Data'] = pd.to_datetime(df_a['Data'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
        except:
            print("")
        df_a = df_a.iloc[::-1].reset_index(drop=True)
        df_a['Data'] = pd.to_datetime(df_a['Data'], format='%Y-%m-%d')

        df_a['Último'] = df_a['Último'].str.replace(',', '.').astype(float)
        df_a['Abertura'] = df_a['Abertura'].str.replace(',', '.').astype(float)
        df_a['Máxima'] = df_a['Máxima'].str.replace(',', '.').astype(float)
        df_a['Mínima'] = df_a['Mínima'].str.replace(',', '.').astype(float)

        #Pegando apenas colunas necessárias
        df_a = df_a[["Data","Último","Abertura","Máxima","Mínima"]]

        #Renomeando Colunas
        df_a.rename(columns={"Data": "Date","Último": "close_TIOc1", "Máxima": "high_TIOc1", "Mínima": "low_TIOc1", "Abertura": "open_TIOc1"}, inplace=True)
        return df_a

    def ajusting_tables_dolar(self,df):
        df_a = df

        df_a = df_a[["Price","High","Low","Open","Close"]]

        df_a = df_a.drop([0, 1], axis=0)

        df_a.rename(columns={"Price":"Date","Close": "close_dolar", "High": "high_dolar", "Low": "low_dolar", "Open": "open_dolar"}, inplace=True)
        df_a = df_a.reset_index(drop=True)
        return df_a

    def ajusting_tables_action(self,df):
        df_b = df

        df_b = df_b[["Price","High","Low","Open","Volume","Close"]]

        df_b = df_b.drop([0, 1], axis=0)

        df_b.rename(columns={"Price":"Date","Close": "close_cmin3", "High": "high_cmin3", "Low": "low_cmin3", "Open": "open_cmin3", "Volume": "volume_cmin3"}, inplace=True)
        df_b = df_b.reset_index(drop=True)

        return df_b

    def ajust_dates(self,df_a, df_b, df_c):
        
        #Ajustando o shape, por conta de feriados e a bolsa não funcionar, é necessário deixa-los com o mesmo tamanho
        df_a['Date'] = pd.to_datetime(df_a['Date'], format='%Y-%m-%d')
        df_b['Date'] = pd.to_datetime(df_b['Date'], format='%Y-%m-%d')
        df_c['Date'] = pd.to_datetime(df_c['Date'], format='%Y-%m-%d')

        common_dates_b_c = set(df_b['Date']).intersection(df_c['Date'])

        # Filtre ambos os DataFrames para manter apenas as linhas com datas comuns
        df_b = df_b[df_b['Date'].isin(common_dates_b_c)]
        df_c = df_c[df_c['Date'].isin(common_dates_b_c)]

        # (Opcional) Resetar os índices, já que as linhas foram filtradas
        df_b.reset_index(drop=True, inplace=True)
        df_c.reset_index(drop=True, inplace=True)
        
        common_dates_a_c = set(df_a['Date']).intersection(df_c['Date'])
        
        df_a = df_a[df_a['Date'].isin(common_dates_a_c)]
        df_c = df_c[df_c['Date'].isin(common_dates_a_c)]

        df_a.reset_index(drop=True, inplace=True)
        df_c.reset_index(drop=True, inplace=True)
        
        #Jutando dataframes
        midle = pd.merge(df_a,df_c, on="Date", how="inner")

        df_to_process = pd.merge(midle,df_b, on="Date", how="inner").drop(columns=['Date'])
        return df_to_process

    def process_and_save_files(self):
        df_cmin3_origin = pd.read_csv(self.path_files_original + "/cmin3.csv")
        df_cmin3_origin_test_model_origin = pd.read_csv(self.path_files_original + "/cmin3_test_model.csv")
        df_usdt_to_brl_origin = pd.read_csv(self.path_files_original + "/usdt2brl.csv")
        df_iron_ore_TIOc1_origin = pd.read_csv(self.path_files_original + "/Contrato Futuro Minério de ferro refinado TIOc1.csv")
        df_iron_ore_DCIOF5_origin = pd.read_csv(self.path_files_original + "/Contrato Futuro Minério de ferro refinado DCIOF5.csv")
        

        df_dolar = self.ajusting_tables_dolar(df_usdt_to_brl_origin)
        df_cmin3 = self.ajusting_tables_action(df_cmin3_origin)
        df_cmin3_test_model = self.ajusting_tables_action(df_cmin3_origin_test_model_origin)
        df_tioc1 = self.ajusting_tables_TIOc1(df_iron_ore_TIOc1_origin)
        df_process = self.ajust_dates(df_dolar, df_cmin3, df_tioc1)
        
        df_dolar.to_csv(self.path_files_process + "/usdt2brl.csv", index=False)
        df_cmin3.to_csv(self.path_files_process + "/cmin3.csv", index=False)
        df_cmin3_test_model.to_csv(self.path_files_process + "/cmin3_test_model.csv", index=False)
        df_tioc1.to_csv(self.path_files_process + "/tioc1.csv", index=False)

        return df_process

    def return_test_values_action(self):
        return pd.read_csv(self.path_files_process + "/cmin3_test_model.csv")

