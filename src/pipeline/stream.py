import streamlit as st
import pandas as pd
import pickle
import torch
from torchvision import models, transforms
from PIL import Image
import json
import matplotlib.pyplot as plt
import time
from pytorch_grad_cam import GradCAM, GradCAMPlusPlus
from pytorch_grad_cam.utils.image import show_cam_on_image
import cv2
import numpy as np

# Modelo Load
resnet34_model = models.resnet34(num_classes=2)
resnet34_model.load_state_dict(torch.load('./src/modelo_resnet34.pth', map_location=torch.device('cpu')))
resnet34_model.eval()
#Function zone 
# Función para aplicar Grad-CAM
def apply_gradcam(model, image, target_layer):
    model.eval()
    gradients = []
    activations = []

    # Función hook para capturar gradientes y activaciones
    def backward_hook(module, grad_in, grad_out):
        gradients.append(grad_out[0])

    def forward_hook(module, input, output):
        activations.append(output)

    # Registrar hooks
    handle_backward = target_layer.register_backward_hook(backward_hook)
    handle_forward = target_layer.register_forward_hook(forward_hook)

    # Hacer predicción y calcular gradiente
    output = model(image)
    target_class = output.argmax(dim=1)
    loss = output[0, target_class]
    model.zero_grad()
    loss.backward()

    # Procesar activaciones y gradientes
    gradients = gradients[0].detach()
    activations = activations[0].detach()
    pooled_gradients = torch.mean(gradients, dim=[0, 2, 3])
    for i in range(activations.shape[1]):
        activations[:, i, :, :] *= pooled_gradients[i]

    heatmap = torch.mean(activations, dim=1).squeeze()
    heatmap = np.maximum(heatmap.cpu().numpy(), 0)
    heatmap /= heatmap.max() if heatmap.max() != 0 else 1

    handle_backward.remove()
    handle_forward.remove()

    return heatmap

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

    def load_json_data():
        with open(url_json_data, 'r') as f:
            json_data = json.load(f)
        df = pd.DataFrame(json_data)
        # Set 'timestamp_pred' as the index column
        df['timestamp_pred'] = pd.to_datetime(df['timestamp_pred']) 
        return df

    # Load data and display preview
    df = load_json_data()
    st.write("Data Preview:", df.tail(10))

    # Show the prediction and timestamp at the top of the page
    st.write("Prediction and Timestamp")
    st.write(df[['prediction', 'timestamp_pred']].tail(10))

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
    time.sleep(5)  
    st.rerun()
elif page == "Upload Photo":
    st.header("Upload Photo for ResNet34 Model")
    
    # Upload photo
    uploaded_file = st.file_uploader("Choose an image...", type="jpg")
    if uploaded_file is not None:
        image = Image.open(uploaded_file)
        show_img=image
        # Preprocess the image
        image = transform(image).unsqueeze(0)
        # Aplicar el modelo ResNet34
        resnet34_model.eval()
        target_layer = resnet34_model.layer4[-1]  # Última capa convolucional del modelo
        heatmap = apply_gradcam(resnet34_model, image, target_layer)
        # Redimensionar el mapa de calor para que coincida con la imagen
        heatmap = np.uint8(255 * heatmap)
        heatmap_colored = cv2.applyColorMap(heatmap, cv2.COLORMAP_JET)
          # Redimensionar el colormap para que coincida con la imagen original
        heatmap_colored = cv2.resize(heatmap_colored, show_img.size)

        # Superponer mapa de calor sobre la imagen original
        overlay = np.array(show_img.convert("RGB"))
        superimposed_img = cv2.addWeighted(heatmap_colored, 0.6, overlay, 0.4, 0)
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
        # Mostrar imagen con mapa de calor
        st.image(superimposed_img, caption="Grad-CAM Visualization", use_container_width=True)