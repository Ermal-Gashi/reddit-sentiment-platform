import torch
from sentence_transformers import SentenceTransformer

print("🔍 CUDA available:", torch.cuda.is_available())
if torch.cuda.is_available():
    print(" Device name:", torch.cuda.get_device_name(0))
    print(" Current device index:", torch.cuda.current_device())
else:
    print(" CUDA not detected — running on CPU")

# Optional: test a tiny embedding to confirm GPU is actually used
print("\nTesting SentenceTransformer on available device...")
device = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer("all-MiniLM-L6-v2", device=device)
emb = model.encode(["This is a GPU test sentence."], show_progress_bar=False)
print(" Embedding shape:", emb.shape)
print(" Finished without errors.")
