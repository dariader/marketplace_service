import pandas as pd

df = pd.read_xml(
    "../offer_data.xml",
    xpath="// offer",
)
print(df.columns)
