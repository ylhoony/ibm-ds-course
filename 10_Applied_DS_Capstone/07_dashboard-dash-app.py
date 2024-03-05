# Import required libraries
import pandas as pd
import dash
from dash import html
from dash import dcc
# import dash_html_components as html
# import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.express as px

# Read the airline data into pandas dataframe
spacex_df = pd.read_csv("./spacex_launch_dash.csv")
max_payload = spacex_df['Payload Mass (kg)'].max()
min_payload = spacex_df['Payload Mass (kg)'].min()

# Create a dash application
app = dash.Dash(__name__)

# Create an app layout
app.layout = html.Div(children=[html.H1('SpaceX Launch Records Dashboard',
                                        style={'textAlign': 'center', 'color': '#503D36', 'font-size': 40}),
                                # TASK 1: Add a dropdown list to enable Launch Site selection
                                # The default select value is for ALL sites
                                # dcc.Dropdown(id='site-dropdown',...)
                                html.Div([dcc.Dropdown(id='site-dropdown',
                                                        options=[{'label': 'All Sites', 'value': 'ALL'},
                                                                {'label': 'CCAFS LC-40', 'value': 'CCAFS LC-40'},
                                                                {'label': 'VAFB SLC-4E', 'value': 'VAFB SLC-4E'},
                                                                {'label': 'KSC LC-39A', 'value': 'KSC LC-39A'},
                                                                {'label': 'CCAFS SLC-40', 'value': 'CCAFS SLC-40'},],
                                                        value='ALL',
                                                        placeholder="Select launch site",
                                                        searchable=True,
                                                        style={"padding": "3px", "fontSize": "20px", "textAlignLast": "center", "width": "100%"}),],
                                        style={"align-items": "center", "display": "flex", "justify-content": "center"}
                                ),
                                html.Br(),

                                # TASK 2: Add a pie chart to show the total successful launches count for all sites
                                # If a specific launch site was selected, show the Success vs. Failed counts for the site
                                html.Div(dcc.Graph(id='success-pie-chart')),
                                html.Br(),

                                html.P("Payload Range (Kg):"),
                                # TASK 3: Add a slider to select payload range
                                #dcc.RangeSlider(id='payload-slider',...)
                                html.Div([
                                    dcc.RangeSlider(id='payload-slider',
                                                    min=0,
                                                    max=10000, 
                                                    step=1000,
                                                    marks={0: '0', 100: '100'},
                                                    value=[min_payload, max_payload],
                                                    ), ],
                                ),

                                # TASK 4: Add a scatter chart to show the correlation between payload and launch success
                                html.Div(dcc.Graph(id='success-payload-scatter-chart')),
                                ])

# TASK 2:
# Add a callback function for `site-dropdown` as input, `success-pie-chart` as output
# Function decorator to specify function input and output
@app.callback([Output(component_id='success-pie-chart', component_property='figure'),
                Output(component_id='success-payload-scatter-chart', component_property='figure')],
                [Input(component_id='site-dropdown', component_property='value'),
                Input(component_id="payload-slider", component_property="value")])

def get_charts(entered_site, entered_payload):
    payload_from, payload_to = entered_payload

    if entered_site == 'ALL':
        pie_fig = px.pie(spacex_df, 
                        values='class', 
                        names='Launch Site', 
                        labels=["class"],
                        title='Launch Success Count for All Sites')
        pie_fig.update_traces(textposition='inside', textinfo='value+percent')

        df_payload = spacex_df[(spacex_df['Payload Mass (kg)'] > payload_from) & (spacex_df['Payload Mass (kg)'] < payload_to)]
        scatter_fig = px.scatter(df_payload, 
                                x='Payload Mass (kg)',
                                y='class', 
                                color='Booster Version Category',
                                title='Payload vs. Launch Outcome for All Launch Site'
                                )
    else:
        # return the outcomes piechart for a selected site
        filtered_df = spacex_df[spacex_df['Launch Site'] == entered_site]
        data = filtered_df['class'].value_counts().to_frame().reset_index()
        print(data)
        pie_fig = px.pie(data, 
                        values='count', 
                        names='class',
                        title=f'Launch Success Ratio at Launch site - {entered_site}')
        pie_fig.update_traces(textposition='inside', textinfo='value+percent')

        df_payload = filtered_df[(filtered_df['Payload Mass (kg)'] > payload_from) & (filtered_df['Payload Mass (kg)'] < payload_to)]
        scatter_fig = px.scatter(df_payload, 
                                x='Payload Mass (kg)',
                                y='class', 
                                color='Booster Version Category',
                                title=f'Success by Payload and Launch site - {entered_site}'
                                )
    


    return [pie_fig, scatter_fig]

# TASK 4:
# Add a callback function for `site-dropdown` and `payload-slider` as inputs, `success-payload-scatter-chart` as output


# Run the app
if __name__ == '__main__':
    app.run_server()
