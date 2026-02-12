
import sys
import os

# Add src to path
sys.path.append(os.path.abspath("src"))

from leakharvester.domain.rules import detect_column_mapping

def check_ambiguity(cols, has_header=True):
    print(f"Testing columns: {cols}")
    has_essentials = False
    if has_header:
        # Simulate logic in ingestor.py around line 316
        mapping = detect_column_mapping(cols)
        mapped_values = list(mapping.values())
        print(f"Mapped values: {mapped_values}")
        
        # Original logic:
        has_essentials = "email" in mapped_values and "password" in mapped_values
        print(f"Has essentials (original): {has_essentials}")
        
        # Check if ambiguous
        is_ambiguous = not cols or ("unknown" in cols and not has_essentials) or (len(cols) > 2 and not has_essentials)
        print(f"Is ambiguous (original): {is_ambiguous}")
        
        if is_ambiguous:
            print("RESULT: REJECTED")
        else:
            print("RESULT: ACCEPTED")
            
        print("-" * 20)

if __name__ == "__main__":
    # Case 1: Email + Password (Should be ACCEPTED)
    check_ambiguity(["Email", "Password"])
    
    # Case 2: User + Password (Should be ACCEPTED because len <= 2)
    check_ambiguity(["User", "Password"])
    
    # Case 3: Email + Password + Date (Should be ACCEPTED)
    check_ambiguity(["Email", "Password", "Date"])
    
    # Case 4: User + Password + Date (Currently REJECTED because len > 2 and has_essentials is False)
    check_ambiguity(["User", "Password", "Date"])
