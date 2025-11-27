import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
df = pd.read_csv("data/cleaned/business_licences_1997_2024.csv")

# Parse dates
df['issued_date'] = pd.to_datetime(df['issueddate'], errors='coerce', utc=True)
df['issued_date'] = df['issued_date'].dt.tz_localize(None)
df['expired_date'] = pd.to_datetime(df['expireddate'], errors='coerce', utc=True)
df['expired_date'] = df['expired_date'].dt.tz_localize(None)

# Calculate business duration (years)
df['duration_years'] = (df['expired_date'] - df['issued_date']).dt.days / 365.25

# Define crisis periods
CRISES = {
    "Dot-Com Crash": ("2000-01-01", "2002-12-31"),
    "Great Recession": ("2008-01-01", "2009-12-31"),
    "Oil Price Crash": ("2014-07-01", "2016-12-31"),
    "COVID-19": ("2020-03-01", "2021-12-31"),
    "Interest Rate Shock": ("2022-01-01", "2023-12-31")
}

def identify_crisis(date):
    if pd.isna(date):
        return "None"
    for crisis_name, (start, end) in CRISES.items():
        if start <= date.strftime('%Y-%m-%d') <= end:
            return crisis_name
    return "Normal"

df['crisis_period'] = df['issued_date'].apply(identify_crisis)

# Create target: Did business survive crisis? (binary)
# If issued during crisis and expired during/shortly after = 0 (failed)
# If issued during crisis and still active or expired much later = 1 (survived)

def label_survival(row):
    if row['crisis_period'] == "Normal":
        return None  # Skip non-crisis businesses for now
    
    if pd.isna(row['duration_years']):
        return 1  # Still active = survived
    
    # If business lasted less than 2 years during crisis = failed
    if row['duration_years'] < 2:
        return 0
    else:
        return 1

df['survived'] = df.apply(label_survival, axis=1)

# Filter to only crisis-period businesses
crisis_df = df[df['crisis_period'] != "Normal"].copy()
crisis_df = crisis_df[crisis_df['survived'].notna()]

print(f"Total businesses during crises: {len(crisis_df):,}")
print(f"Survived: {(crisis_df['survived'] == 1).sum():,}")
print(f"Failed: {(crisis_df['survived'] == 0).sum():,}")

# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

# Select features
feature_cols = ['businesstype', 'businesssubtype', 'numberofemployees', 'crisis_period', 'localarea']

# Clean and encode
for col in feature_cols:
    if col in crisis_df.columns:
        crisis_df[col] = crisis_df[col].fillna('unknown')
        crisis_df[col] = crisis_df[col].astype(str).str.lower().str.strip()

# Encode categorical variables
le_dict = {}
for col in ['businesstype', 'businesssubtype', 'crisis_period', 'localarea']:
    if col in crisis_df.columns:
        le = LabelEncoder()
        crisis_df[f'{col}_encoded'] = le.fit_transform(crisis_df[col])
        le_dict[col] = le

# Handle employees
if 'numberofemployees' in crisis_df.columns:
    crisis_df['numberofemployees'] = pd.to_numeric(crisis_df['numberofemployees'], errors='coerce')
    crisis_df['numberofemployees'] = crisis_df['numberofemployees'].fillna(crisis_df['numberofemployees'].median())

# Prepare feature matrix
X_cols = ['businesstype_encoded', 'businesssubtype_encoded', 'numberofemployees', 
          'crisis_period_encoded', 'localarea_encoded']
X_cols = [col for col in X_cols if col in crisis_df.columns]

X = crisis_df[X_cols]
y = crisis_df['survived']

print(f"\nFeatures used: {X_cols}")
print(f"Feature matrix shape: {X.shape}")

# =============================================================================
# TRAIN MODEL
# =============================================================================

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Train Random Forest
rf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced')
rf.fit(X_train, y_train)

# Predictions
y_pred = rf.predict(X_test)

# =============================================================================
# EVALUATE MODEL
# =============================================================================

print("\n" + "="*70)
print("MODEL PERFORMANCE")
print("="*70)
print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Failed', 'Survived']))

# Feature importance
feature_importance = pd.DataFrame({
    'feature': X_cols,
    'importance': rf.feature_importances_
}).sort_values('importance', ascending=False)

print("\nFeature Importance:")
print(feature_importance)

# =============================================================================
# ANALYZE VULNERABILITY BY BUSINESS TYPE
# =============================================================================

print("\n" + "="*70)
print("BUSINESS TYPE VULNERABILITY ANALYSIS")
print("="*70)

# Get predictions for all business types
crisis_df['predicted_survival_prob'] = rf.predict_proba(X)[:, 1]

# Group by business type
vulnerability = crisis_df.groupby('businesstype').agg({
    'survived': ['mean', 'count'],
    'predicted_survival_prob': 'mean'
}).round(3)

vulnerability.columns = ['actual_survival_rate', 'count', 'predicted_survival_rate']
vulnerability = vulnerability[vulnerability['count'] >= 10]  # Filter small samples
vulnerability = vulnerability.sort_values('actual_survival_rate')

print("\nMost Vulnerable Business Types (lowest survival rate):")
print(vulnerability.head(15))

print("\nMost Resilient Business Types (highest survival rate):")
print(vulnerability.tail(15))

# =============================================================================
# VISUALIZATIONS
# =============================================================================

# 1. Feature Importance Plot
fig, ax = plt.subplots(figsize=(10, 6))
feature_importance_plot = feature_importance.head(10)
ax.barh(feature_importance_plot['feature'], feature_importance_plot['importance'])
ax.set_xlabel('Importance Score', fontweight='bold')
ax.set_title('Top 10 Features Predicting Business Survival During Crises', fontweight='bold', pad=20)
ax.invert_yaxis()
plt.tight_layout()
plt.savefig('data/cleaned/feature_importance.png', dpi=300, bbox_inches='tight')
plt.show()

# 2. Business Type Vulnerability
fig, ax = plt.subplots(figsize=(12, 8))
top_vulnerable = vulnerability.head(20)
colors = ['red' if x < 0.5 else 'orange' if x < 0.7 else 'green' 
          for x in top_vulnerable['actual_survival_rate']]
ax.barh(range(len(top_vulnerable)), top_vulnerable['actual_survival_rate'], color=colors, alpha=0.7)
ax.set_yticks(range(len(top_vulnerable)))
ax.set_yticklabels(top_vulnerable.index)
ax.set_xlabel('Survival Rate During Crises', fontweight='bold')
ax.set_title('Most Vulnerable Business Types During Economic Crises', fontweight='bold', pad=20)
ax.axvline(0.5, color='black', linestyle='--', linewidth=1, alpha=0.5)
plt.tight_layout()
plt.savefig('data/cleaned/business_vulnerability.png', dpi=300, bbox_inches='tight')
plt.show()

print("\n✓ Saved: feature_importance.png")
print("✓ Saved: business_vulnerability.png")

# =============================================================================
# FUTURE PREDICTIONS
# =============================================================================

print("\n" + "="*70)
print("PREDICTIONS FOR NEXT CRISIS")
print("="*70)

print("\nIf a recession similar to 2008 Great Recession occurs:")
print("Businesses most at risk:")

# You can create hypothetical scenarios here
# For example: simulate a new crisis and predict which current business types would fail