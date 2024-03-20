import pickle
import pandas as pd

# Load the saved XGBoost classifier model using pickle. Models are saved in 'Models' folder.
with open('Models\\Churn Model V1.pkl', 'rb') as classification_model_file:
    classification_model = pickle.load(classification_model_file)

# Create a feature dataset using the final_dataset.csv file. The file contains encoded data of the profile.
df = pd.read_csv(r'encoded_data\final_dataset.csv')
X = df.drop(["profile_name"], axis=1)

# Save predictions
job_change_predictions = classification_model.predict_proba(X)

print('Turnover Probability: ', job_change_predictions[0][1])

# Make job change horizon predictions.
if job_change_predictions[0][1] >= job_change_predictions[0][0]:
    with open('Models\\Horizon Model V1.pkl', 'rb') as regression_model_file:
        regression_model = pickle.load(regression_model_file)

    job_change_horizon_predictions = regression_model.predict(X)

    if job_change_horizon_predictions == 1:
        print('Turnover Horizon: 6-12 months\n')
    else:
        print('Turnover Horizon: 0-6 months\n')

else:
    print("Employee is not likely to turnover\n")
