# Create test_llm_direct.py
from src.utils import call_groq_llm

# Test with a simple review
result = call_groq_llm("This dress is beautiful and fits perfectly!")
print(result)

# Test with another
result2 = call_groq_llm("Terrible quality, very disappointed.")
print(result2)