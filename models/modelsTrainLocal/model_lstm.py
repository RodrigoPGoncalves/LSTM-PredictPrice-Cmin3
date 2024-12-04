import mlflow
import mlflow.keras
from mlflow.models.signature import infer_signature
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
from datetime import datetime
import pandas as pd
import os
from tensorflow.keras.regularizers import l1, l2, l1_l2

class Model:
    def __init__(self, df_process, sequence_length = 5, batch_size = 32, epochs = 32):
        self.target = "close_cmin3"
        self.sequence_length = sequence_length
        self.df_process = df_process
        self.features = self.df_process.columns[0:-1].to_list()
        self.scaler = MinMaxScaler()
        self.value_train_split = 0.7
        self.batch_size = batch_size
        self.epochs = epochs
        self.wish_perid_to_predict = 5
        self.df_values = pd.DataFrame(columns=["params", "loss_test", "MAPE", "R2", "RMSE"])
    
    def normalize_data(self):
        self.scaled_data = self.scaler.fit_transform(self.df_process[self.features + [self.target]])
    
    def create_sequences(self):
        X_scalled, y_scalled = [], []
        for i in range(self.sequence_length, len(self.scaled_data)):
            X_scalled.append(self.scaled_data[i-self.sequence_length:i, :-1])  
            y_scalled.append(self.scaled_data[i, -1])    
        self.X, self.y = np.array(X_scalled), np.array(y_scalled)        

    def split_train_test(self):
        train_size = int(len(self.X) * self.value_train_split )
        self.X_train, self.X_test = self.X[:train_size], self.X[train_size:]
        self.y_train, self.y_test = self.y[:train_size], self.y[train_size:]
    
    def sequence_model(self, df_cmin3_valid_model, end_date, params):
        tf.random.set_seed(7)
        np.random.seed(7)

        self.model = Sequential([
                    LSTM(params["hidden_units_1"], return_sequences=True, kernel_regularizer=l2(0.01), activity_regularizer=l1_l2(0.01, 0.01), input_shape=(self.X_train.shape[1], self.X_train.shape[2])),
                    Dropout(params["dropout_rate"]),
                    LSTM(params["hidden_units_2"], return_sequences=True, kernel_regularizer=l2(0.001), activity_regularizer=l1_l2(0.001, 0.001)),
                    Dropout(params["dropout_rate2"]),
                    LSTM(params["hidden_units_3"], return_sequences=False),
                    Dropout(params["dropout_rate3"]),
                    Dense(1)  
                ])
        self.model.compile(optimizer="adam", loss='mean_squared_error')

        #base_dir = "models_experiment"
        #mlflow.set_tracking_uri(f"file://{os.path.abspath(base_dir)}")
        #mlflow.set_experiment("models_and_params")
        mlflow.start_run()
        

        try:
            current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            artifact_path = f"model_{current_date}"

            # Logar parâmetros no MLflow
            mlflow.log_params(params)

            # Treinar o modelo
            history = self.model.fit(self.X_train, self.y_train, batch_size=self.batch_size, epochs=self.epochs, verbose=1)

            # Inferir a assinatura com base nos dados de treinamento
            #signature = infer_signature(self.X_train, self.model.predict(self.X_train))

            # Criar exemplo de entrada para o modelo (usando os primeiros dados de entrada)
            #input_example = pd.DataFrame(self.X_train[0:1].reshape(-1, len(self.features)), columns=self.features)

            # Registrar o modelo no MLflow com assinatura e exemplo de entrada
            #mlflow.keras.log_model(self.model, artifact_path, signature=signature, input_example=input_example)
            mlflow.keras.log_model(self.model, artifact_path)


            # Avaliar o modelo
            loss =  self.model.evaluate(self.X_test, self.y_test)

            # Logar métricas no MLflow
            mlflow.log_metric("loss", loss)
            print(f"Treinamento concluído com perda: {loss}")

            
            self.analise_model()
            self.validation_model(df_cmin3_valid_model, end_date)

        except Exception as e:
            print(f"Erro durante o treinamento: {e}")
        finally:
            # Finalizar o experimento
            mlflow.end_run()

    def analise_model(self):

        self.predictions = self.model.predict(self.X_test)
        mae = mean_absolute_error(self.y_test, self.predictions)
        mse = mean_squared_error(self.y_test, self.predictions)
        rmse = np.sqrt(mse)
        r2 = r2_score(self.y_test, self.predictions)
        epsilon = 1e-10  # Evitar divisão por zero
        mape = np.mean(np.abs((self.y_test - self.predictions.ravel()) / (self.y_test + epsilon))) * 100

        # Logar métricas no MLflow
        mlflow.log_metrics({"MAE": mae, "MSE": mse, "RMSE": rmse,"MAPE": mape, "R²": r2})
        print(f"MAE: {mae:.4f}, MSE: {mse:.4f}, RMSE: {rmse:.4f} MAPE: {mape:.2f}, R²: {r2:.4f}")

        # Rescale only the predicted close price
        self.predictions = self.model.predict(self.X_test)
        self.rescaled_predictions = self.scaler.inverse_transform(
            np.hstack((np.zeros((len(self.predictions), len(self.features))), self.predictions.reshape(-1, 1)))
        )[:, -1]  

        # Rescale actual values
        self.actual_close = self.scaler.inverse_transform(
            np.hstack((np.zeros((len(self.y_test), len(self.features))), self.y_test.reshape(-1, 1)))
        )[:, -1]

        # Plot comparison
        plt.figure(figsize=(14, 7))
        plt.plot(self.actual_close, label='Actual Close Prices', color='blue')
        plt.plot(self.rescaled_predictions, label='Predicted Close Prices', color='red', linestyle='dashed')
        plt.title('Actual vs Predicted Stock Close Prices')
        plt.xlabel('Time Steps')
        plt.ylabel('Close Price')
        plt.legend()
        plt.grid(True)
        plt.savefig('model_train_test.png', format='png', dpi=300)  # Escolha o formato e a resolução
        mlflow.log_artifact("model_train_test.png")
        #plt.show()
        
    def validation_model(self, df_cmin3_valid_model, end_date):
        df_cmin3_test_model = df_cmin3_valid_model[["Date",self.target]]

        #Ultimo sequence_length valores usados para o treinamento
        last_sequence = self.scaled_data[-self.sequence_length:, :-1] #Sem a coluna target
        future_predictions = []

        for _ in range(self.wish_perid_to_predict):
            last_sequence_reshaped = np.expand_dims(last_sequence, axis=0)
            
            next_prediction = self.model.predict(last_sequence_reshaped)[0, 0]  # Saída única
            
            future_predictions.append(next_prediction)
            
            next_sequence = np.append(last_sequence[1:], [[*last_sequence[-1, :-1], next_prediction]], axis=0)
            last_sequence = next_sequence

        # Reverter a escala das previsões para a escala original
        rescaled_future_predictions = self.scaler.inverse_transform(
            np.hstack((np.zeros((len(future_predictions), len(self.features))), np.array(future_predictions).reshape(-1, 1)))
        )[:, -1]
        
        prediction_dates = pd.date_range(start='2024-11-21', periods=self.wish_perid_to_predict, freq='B')
        #filter_dates_hist = self.df_process[self.df_process["Date"] >= "2024-10-01"]
        filter_dates_hist = pd.Series(pd.date_range(start='2024-10-01', end=end_date, freq='B'))
        combined_dates = pd.concat([filter_dates_hist,df_cmin3_test_model["Date"]])

        values_actual = np.concatenate([self.actual_close[-filter_dates_hist.count():], df_cmin3_test_model[self.target].to_numpy()])

        # Plotar os resultados
        plt.figure(figsize=(14, 7))
        plt.plot(combined_dates, values_actual, label='Dados Históricos', color='blue', marker='o')

        #plt.plot(filter_dates_hist['Date'], self.rescaled_predictions[-filter_dates_hist[self.target].count():], label='Dados Previstos', color='red', marker='o')
        plt.plot(filter_dates_hist, self.rescaled_predictions[-filter_dates_hist.count():], label='Dados Previstos', color='red', marker='o')
        
        plt.plot(prediction_dates, rescaled_future_predictions, label='Dados Previstos', color='orange', marker='o')
        plt.title('Previsões de Preço de Fechamento')
        plt.xlabel('Datas')
        plt.ylabel('Preço de Fechamento')
        plt.legend()
        plt.grid(True)
        plt.savefig('previsao.png', format='png', dpi=300)
        mlflow.log_artifact("previsao.png")
        #plt.show()
                    

