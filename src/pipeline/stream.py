import streamlit as st
import pandas as pd
import pickle
import torch
from torchvision import models, transforms
from PIL import Image
import json
import matplotlib.pyplot as plt

# Modelo Load
resnet34_model = models.resnet34(num_classes=2)
resnet34_model.load_state_dict(torch.load('./src/modelo_resnet34.pth', map_location=torch.device('cpu')))
resnet34_model.eval()

# Class zone


class ConvertToRGB:
    def __call__(self, img):
        if img.mode != "RGB":
            img = img.convert("RGB")
        return img

# Define the image transformation as in the model training
transform = transforms.Compose([
    ConvertToRGB(),
    transforms.ToTensor(),
    transforms.Resize((704,256)),
    transforms.Normalize((0.5,), (0.25,))
])

# Define the Streamlit app
st.title("Predictive Maintenance App")
with st.expander("Sobre este reto"):
    st.markdown("**¿Quiénes son los desarroladores:?**")
    st.info("Los creadores son Beñat Alcain Epelde, Ana Alonso Jirout y Adrián Ruiz Donado de la primera promoción del máster de IA aplicada ")
    st.markdown("**Como usar la app?**")
    st.warning("Selecciona lo que deseas conocer en el sidebar de la izquierda ")
# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Machine Learning Model", "Upload Photo"])

# Pagina ML
if page == "Machine Learning Model":
    st.header("Apply Machine Learning Model")
    st.markdown(
        "Este dashboard muestra las series temporales de métricas capturadas por 8 sensores. Cada gráfico representa una métrica específica."
    )

    # Load JSON data
    url_json_data = r"src\pipeline\sensor_data.json"
    with open(url_json_data, 'r') as f:
        json_data = json.load(f)
    df = pd.DataFrame(json_data)
    
    # Set 'timestamp_predict' as the index column
    df['timestamp_pred'] = pd.to_datetime(df['timestamp_pred']) 
    #df.set_index('timestamp_pred', inplace=True) 
    st.write("Data Preview:", df.tail(10))
    # Show the prediction and timestamp at the top of the page
    st.write("Prediction and Timestamp")
    st.write(df[['prediction','timestamp_pred']].tail(10))
    
    # Group metrics by sensor
    sensors = sorted(set(col.split('_')[0] for col in df.columns if col.startswith('S') and col != 'prediction'))

    # Plotting
    st.subheader("Visualización de Series Temporales por Sensor")
    for sensor in sensors:
        sensor_metrics = [col for col in df.columns if col.startswith(sensor)]
        st.markdown(f"### Sensor {sensor}")
        
        fig, ax = plt.subplots(figsize=(10, 6))
        for metric in sensor_metrics:
            ax.plot(df['timestamp_pred'], df[metric], label=metric)
        
        ax.set_title(f"Métricas del Sensor {sensor}")
        ax.set_xlabel("Tiempo")
        ax.set_ylabel("Valor")
        ax.legend()
        ax.grid(True)

        st.pyplot(fig)
elif page == "Upload Photo":
    st.header("Upload Photo for ResNet34 Model")
    
    # Upload photo
    uploaded_file = st.file_uploader("Choose an image...", type="jpg")
    if uploaded_file is not None:
        image = Image.open(uploaded_file)
        show_img=image
        # Preprocess the image
        image = transform(image).unsqueeze(0)
        
        # Apply the ResNet34 model
        with torch.no_grad():
            outputs = resnet34_model(image)
        
        _, predicted = torch.max(outputs, 1)
        if predicted.item() == 0:
            st.success("La pieza está en buen estado.")
            st.image(show_img, caption=f'{show_img.info}', use_container_width=True)
        else:
            st.error('La pieza tiene un defecto', icon="🚨")
            st.image(show_img, caption=f'{show_img.info}', use_container_width=True)