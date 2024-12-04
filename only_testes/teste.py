"""import pandas as pd
import preprocess as pp
import matplotlib.pyplot as plt

pp_object = pp.PreProcess()
path_files_original = "originalFiles"
path_files_process = "preprocessFiles"
df_usdt_to_brl_origin = pd.read_csv(path_files_original + "/usdt2brl.csv")
print(df_usdt_to_brl_origin)

df_b = df_usdt_to_brl_origin

df_b = df_b[["Price","High","Low","Open","Volume","Close"]]

df_b = df_b.drop([0, 1], axis=0)

df_b.rename(columns={"Price":"Date","Close": "close_cmin3", "High": "high_cmin3", "Low": "low_cmin3", "Open": "open_cmin3", "Volume": "volume_cmin3"}, inplace=True)
df_b = df_b.reset_index(drop=True)

print(df_b["Date"].dtypes)  # Exibe o tipo original (provavelmente 'object')

df_b['Date'] = pd.to_datetime(df_b['Date'], format='%Y-%m-%d')

print(df_b["Date"].dtypes)  # 'datetime64[ns]'

prediction_dates = ['2024-11-21', '2024-11-22', '2024-11-25', '2024-11-26',
               '2024-11-27', '2024-11-28', '2024-11-29']
rescaled_future_predictions = [5.40237613,5.42115472, 5.44440088, 5.45496668, 5.44689519, 5.43023214,5.41287228]

plt.figure(figsize=(14, 7))

plt.plot(prediction_dates, rescaled_future_predictions, label='Dados Previstos', color='orange', marker='o')
plt.title('Previsões de Preço de Fechamento')
plt.xlabel('Datas')
plt.ylabel('Preço de Fechamento')
plt.legend()
plt.grid(True)
plt.show()"""


"""import random

param_dist = {
    "sequence_length": [3, 5, 10, 20],
    "hidden_units_1": [20, 50, 100, 200], 
    "hidden_units_2": [20, 50, 100, 200],  
    "hidden_units_3": [20, 50, 100, 200],  
    "dropout_rate": [0.2, 0.3, 0.4],   
    "dropout_rate2": [0.2, 0.3, 0.4],  
    "dropout_rate3": [0.2, 0.3, 0.4],    
    "batch_size": [32, 64, 128],      
    "epochs": [30, 50, 100],           
    "optimizer": ["adam"],     
}


for i in range(0,2):
    params = {
        "sequence_length": random.choice(param_dist["sequence_length"]),
        "hidden_units_1": random.choice(param_dist["hidden_units_1"]),
        "hidden_units_2": random.choice(param_dist["hidden_units_2"]),
        "hidden_units_3": random.choice(param_dist["hidden_units_3"]),
        "dropout_rate": random.choice(param_dist["dropout_rate"]),
        "dropout_rate2": random.choice(param_dist["dropout_rate2"]),
        "dropout_rate3": random.choice(param_dist["dropout_rate3"]),
        "batch_size": random.choice(param_dist["batch_size"]),
        "epochs": random.choice(param_dist["epochs"]),
        "optimizer": random.choice(param_dist["optimizer"])
    }
    print(params["sequence_length"])
"""

import yfinance as yf
import mlflow


logged_model = 'runs:/14b9b7667e334083b36e762dff556afc/model_2024-12-02_03-31-27'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)
