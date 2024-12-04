import mlflow
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import os

class Model():
    def __init__(self,pp_object):
        self.sequence_length = 3
        self.sequence_length_lstm_bi_atten_cnn = 7
        self.target = "close_cmin3"
        self.array_models = []    
        self.scaler = MinMaxScaler()
        self.pp_object = pp_object

    def train_model(self):
        pass

    def load_models_mlflow(self):
        try:
            script_directory = os.path.dirname(os.path.abspath(__file__))
            model_path1 = script_directory.rstrip("/models") + "/mlruns/0/14b9b7667e334083b36e762dff556afc/artifacts/model_2024-12-02_03-31-27"
            model_path2 = script_directory.rstrip("/models") + "/mlruns/0/3a14b4cd0e1b402393a70e1e2c1ec2b8/artifacts/model_2024-12-02_15-34-58"
            model_path3 = script_directory.rstrip("/models") + "/mlruns/0/14cbf1997d4542c3949793474baabbec/artifacts/model_2024-12-02_17-42-17"
            model_path4 = script_directory.rstrip("/models") + "/mlruns/0/3c5ddac6f1204aba9241a6d5faf5b68d/artifacts/model_2024-12-02_18-48-34"
            model_path5 = script_directory.rstrip("/models") + "/mlruns/0/11f651a402524ba59e196de9ac9eec80/artifacts/model_2024-12-02_19-57-04"

            model_sample_lstm = mlflow.pyfunc.load_model(model_path1)
            model_lstm_bidirecional = mlflow.pyfunc.load_model(model_path2)
            model_lstm_attention = mlflow.pyfunc.load_model(model_path3)
            model_lstm_cnn = mlflow.pyfunc.load_model(model_path4)
            model_lstm_bi_atten_cnn = mlflow.pyfunc.load_model(model_path5)
            self.array_models = [model_sample_lstm,
                                model_lstm_bidirecional,
                                model_lstm_attention,
                                model_lstm_cnn,
                                model_lstm_bi_atten_cnn]
            if(len(self.array_models) == 5):
                return True,"Modelos carregados do Mlflow com sucesso"
            return False, "Array de modelos não carregou totalmente"
        except AssertionError as e:
            return False, str(e)

    def predict_period(self, period_to_predict = 5):
        try:
            if(len(self.array_models) == 5):
                list_predictions = []
                df_process = self.pp_object.process_and_save_files()
                df_cmin3_test_model = self.pp_object.return_test_values_action()
                features = df_process.columns[0:-1].to_list()
                scaled_data = self.scaler.fit_transform(df_process[features + [self.target]])
                sequence_length = self.sequence_length

                for i,model in enumerate(self.array_models):
                    if(i == 4):
                        sequence_length = self.sequence_length_lstm_bi_atten_cnn
                        
                    last_sequence = scaled_data[-sequence_length:, :-1] #Sem a coluna target
                    future_predictions = []
                    for i in range(period_to_predict):
                        last_sequence_reshaped = np.expand_dims(last_sequence, axis=0)
                        next_prediction = model.predict(last_sequence_reshaped)[0, 0]  # Saída única
                        future_predictions.append(next_prediction)
                        next_sequence = np.append(last_sequence[1:], [[*last_sequence[-1, :-1], next_prediction]], axis=0)
                        last_sequence = next_sequence



                    # Reverter a escala das previsões para a escala original
                    rescaled_future_predictions = self.scaler.inverse_transform(
                        np.hstack((np.zeros((len(future_predictions), len(features))), np.array(future_predictions).reshape(-1, 1)))
                    )[:, -1]
                    list_predictions.append(rescaled_future_predictions.tolist())
                date_predict = pd.DataFrame(pd.date_range(start=df_cmin3_test_model["Date"][0], periods=period_to_predict, freq='B'), columns=["Date"])
                
                return True, {
                    "df_cmin3_test_model": df_cmin3_test_model[["Date",self.target]].to_dict(orient="records"),
                    "date_predict": date_predict["Date"].dt.strftime('%Y-%m-%d').tolist(),
                    "predict_sample_lstm":list_predictions[0],
                    "predict_lstm_bidirecional":list_predictions[1],
                    "predict_lstm_attention":list_predictions[2],
                    "predict_lstm_cnn":list_predictions[3],
                    "predict_lstm_bi_atten_cnn":list_predictions[4]
                }
            return False, "Array de modelos não carregou totalmente"
        except AssertionError as e:
            return False, str(e)
        
    def adicionar_dias_uteis(self, data_inicial, dias_para_adicionar):
        data_inicial = pd.to_datetime(data_inicial)
        dias_adicionados = 0
        while dias_adicionados < dias_para_adicionar:
            print("aqui")
            print(data_inicial)
            data_inicial += pd.Timedelta(days=1)
            if data_inicial.weekday() < 5:
                dias_adicionados += 1
        
        return data_inicial
    
    def predict_new_values_period(self, data_entries, period_to_predict = 5):
        try:
            if(len(self.array_models) == 5):
                list_predictions = []
                df_process_old = self.pp_object.process_and_save_files()
                new_data = [entry.__dict__ for entry in data_entries]
                new_data_df = pd.DataFrame(new_data)

                df_process = pd.concat([df_process_old, new_data_df], ignore_index=True)
                
                df_cmin3_test_model = self.pp_object.return_test_values_action()
                features = df_process.columns[0:-1].to_list()
                scaled_data = self.scaler.fit_transform(df_process[features + [self.target]])
                sequence_length = self.sequence_length
                
                
                for i,model in enumerate(self.array_models):
                    if(i == 4):
                        sequence_length = self.sequence_length_lstm_bi_atten_cnn
                    
                    last_sequence = scaled_data[-sequence_length:, :-1] #Sem a coluna target
                    future_predictions = []
                    for i in range(period_to_predict):
                        last_sequence_reshaped = np.expand_dims(last_sequence, axis=0)
                        next_prediction = model.predict(last_sequence_reshaped)[0, 0]  # Saída única
                        future_predictions.append(next_prediction)
                        next_sequence = np.append(last_sequence[1:], [[*last_sequence[-1, :-1], next_prediction]], axis=0)
                        last_sequence = next_sequence



                    # Reverter a escala das previsões para a escala original
                    rescaled_future_predictions = self.scaler.inverse_transform(
                        np.hstack((np.zeros((len(future_predictions), len(features))), np.array(future_predictions).reshape(-1, 1)))
                    )[:, -1]
                    list_predictions.append(rescaled_future_predictions.tolist())
                

                data_inicial = pd.to_datetime(df_cmin3_test_model["Date"][0])
                dias_adicionados = 0
                while dias_adicionados < new_data_df.shape[0]:
                    data_inicial += pd.Timedelta(days=1)
                    if data_inicial.weekday() < 5:
                        dias_adicionados += 1

                date_predict = pd.DataFrame(pd.date_range(start=data_inicial, periods=period_to_predict, freq='B'), columns=["Date"])
                return True, {
                    "df_cmin3_test_model": df_cmin3_test_model[["Date",self.target]].to_dict(orient="records"),
                    "date_predict": date_predict["Date"].dt.strftime('%Y-%m-%d').tolist(),
                    "predict_sample_lstm":list_predictions[0],
                    "predict_lstm_bidirecional":list_predictions[1],
                    "predict_lstm_attention":list_predictions[2],
                    "predict_lstm_cnn":list_predictions[3],
                    "predict_lstm_bi_atten_cnn":list_predictions[4]
                }
            return False, "Array de modelos não carregou totalmente"
        except AssertionError as e:
            return False, str(e)
        
    
    