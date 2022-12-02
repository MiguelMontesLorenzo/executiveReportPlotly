import Datacleaning_ETL_pizzasPrediction

#generate graphs
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

#saving images process
import os

#dataframe treatment modules
import pandas as pd
import numpy as np

#generate PDF
from xhtml2pdf import pisa






def report_block_template(report_type, graph_url, caption=''):
    if report_type == 'interactive':
        graph_block = '<iframe style="border: none;" src="{graph_url}.embed" width="100%" height="600px"></iframe>'
    
    elif report_type == 'static':
        graph_block = (''
            '<a href="{graph_url}" target="_blank">' # Open the interactive graph when you click on the image
                '<img style="height: 400px;" src="{graph_url}">'
            '</a>')

    report_block = ('' +
        graph_block +
        '{caption}' + # Optional caption to include below the graph
        '<br>'      + # Line break
        '<a href="{graph_url}" style="color: rgb(190,190,190); text-decoration: none; font-weight: 200;" target="_blank">'+
            'Click to comment and see the interactive graph' + # Direct readers to Plotly for commenting, interactive graph
        '</a>' +
        '<br>' +
        '<hr>') # horizontal line                       

    return report_block.format(graph_url=graph_url, caption=caption)







# Utility function
def convert_html_to_pdf(source_html, output_filename):
    # open output file for writing (truncated binary)
    result_file = open(output_filename, 'w+b')

    # convert HTML to PDF
    pisa_status = pisa.CreatePDF(
            source_html,                # the HTML to convert
            dest=result_file)           # file handle to recieve result


    # close output file
    result_file.close()                 # close output file

    # return True on success and False on errors
    return pisa_status.err



if __name__ == '__main__':


    dfs = Datacleaning_ETL_pizzasPrediction.extract()
    df_ingredients = dfs[-1]
    dfs_pizzas = Datacleaning_ETL_pizzasPrediction.data_cleaning(dfs[1:5])
    dfs_Product_Details = Datacleaning_ETL_pizzasPrediction.dataframe_for_Product_Details_graphs(dfs_pizzas, df_ingredients)


    #DEFINE THE EMPLOYEE WAGES:  (Asuming we have 1 cook and 2 waiters)
    #average cook wage (in USA): 31,630 $
    #average waiter wage (in USA): 29,010 $
    total_employee_wages = 31630+2*29010

    #DEFINE CORPORATE FEDERAL INCOME TAX:
    #feceral corporate income tax: 21%
    #average state corporate income tax: 3.38 %
    tax_percentage = 21+3.38

    df_metrics, df_metrics_by_month, df_week_day = Datacleaning_ETL_pizzasPrediction.dataframe_for_Executive_Summary_graphs(  df_ingredients_costs = df_ingredients,
                                                                                            dfs = dfs_pizzas,
                                                                                            tax_percentage = tax_percentage,
                                                                                            total_employee_wages = total_employee_wages)



    df_product_details = dfs_Product_Details[0]
    df_product_details_by_size = dfs_Product_Details[1]
    df_product_details_by_pizza_type_id = dfs_Product_Details[2]
    df_product_details_by_category = dfs_Product_Details[3]
    df_pizza_marginal_profits = dfs_Product_Details[4]


    #ORDERS BY SIZE
    fig1 = px.bar(  data_frame=df_product_details_by_size,
                    x='size',
                    y='quantity',
                    labels={'Pizza Size':'Orders'},
                    height=600, 
                    text_auto=True,
                    title='Orders by pizza size')
    fig1.update_layout( barmode='relative',
                        title_text='ORDERS BY SIZE',
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        autosize=False, width=1200, height=600)



    fig2 = px.pie(  data_frame=df_product_details_by_category, 
                    values='quantity',
                    names='category')

    fig2.update_layout( title='ORDERS BY CATEGORY',
                        uniformtext_minsize=14,
                        uniformtext_mode='hide')

    
    fig3 = px.bar(  data_frame=df_product_details,
                    y='quantity',
                    x='pizza_type_id',
                    color='size',
                    text='quantity',
                    labels={
                        "quantity": "Orders",
                        "pizza_type_id": "Pizza flavor",
                        "size": "Sizes"
                    })
    fig3.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig3.update_layout( title="PIZZA TYPE'S POPULARITY AND SIZES",
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        xaxis={'categoryorder': 'total ascending'},
                        autosize=False, width=1200, height=600)






    s1 = df_pizza_marginal_profits['price']
    s2 = df_pizza_marginal_profits['pizza_type_cost']*-1
    s3 = df_pizza_marginal_profits['pizza_marginal_profit']
    pizza_type_id = df_pizza_marginal_profits['pizza_type_id'].tolist()

    #EXAMPLE OF make_subplots
    # fig4 = make_subplots(rows=2, cols=1)

    # fig4 = make_subplots(rows=3, cols=1,
    # specs=[ [{'rowspan': 2, 'colspan': 1}],
    #         [None, None, None],
    #         [{'rowspan': 2, 'colspan': 1}]],
    #         vertical_spacing=0.075,
    #         horizontal_spacing=0.08)

    fig4 = make_subplots(rows=3, cols=1,
    specs=[ [{'rowspan': 2, 'colspan': 1}],
            [None],
            [{'rowspan': 1, 'colspan': 1}]],
            vertical_spacing=0.15,
            horizontal_spacing=0.08)
    fig4.add_trace(go.Bar(x=pizza_type_id, y=s1, text = s1, name='Price ($)', marker={'color': '#29E542'}), row=1, col=1)
    fig4.add_trace(go.Bar(x=pizza_type_id, y=s2, text = s2, name='Cost ($)', marker={'color': '#D6453E'}), row=1, col=1)
    fig4.add_trace(go.Bar(x=pizza_type_id, y=s3, text = s3, base=0, name='Marginal Profit ($)', marker={'color': 'rgba(239, 218, 79, 0.5)'}), row=3, col=1)
    fig4.update_layout( barmode='relative',
                        title_text='BENEFIT MARGINS PER PIZZA TYPE',
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        autosize=False, width=1600, height=800)





    df_fig5 = df_metrics[['date','cumulative_sales', 'cumulative_costs', 'gross_profit']]
    fig5 = px.area( data_frame=df_fig5,
                    x='date', 
                    y=df_fig5.columns,
                    color_discrete_map = {'cumulative_sales': '#29E542', 'cumulative_costs': '#D6453E', 'gross_profit':'rgb(239, 218, 79, 0)'},
                    labels={
                        "cumulative_sales": "Gross Sales",
                        "cumulative_costs": "Expenses",
                        "gross_profit": "Gross Profit"
                    },
                    hover_data={'date': '%Y-%m-%d'},
                    title='custom tick labels')
    fig5.update_traces(stackgroup = None, fill = 'tozeroy')
    fig5.update_layout( title_text='GROS PROFIT = GROSS SALES - ExpenseS',
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        autosize=False,width=1200, height=600)
    # fig5.show()





    df_fig6 = df_metrics_by_month[['month', 'sales']]
    fig6 = px.bar( data_frame = df_fig6,
                    x='month',
                    y='sales',
                    text='sales',
                    height=600)
    fig6.update_traces(marker_color='#29E542')
    fig6.update_layout( title_text='MONTHLY GROSS SALES',
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        autosize=False, width=1200, height=800)
    # fig6.show()





    df_fig7 = df_metrics_by_month[['month', 'costs']]
    fig7 = px.bar( data_frame = df_fig7,
                    x='month',
                    y='costs',
                    text='costs',
                    height=600)
    fig7.update_traces(marker_color='#D6453E')
    fig7.update_layout( title_text='MONTHLY EXPENSES',
                        uniformtext_minsize=10,
                        uniformtext_mode='hide',
                        autosize=False, width=1200, height=800)
    # fig7.show()





    df_fig8 = df_week_day[['weekday', 'sales']]
    fig8 = px.bar( data_frame = df_fig8,
                    x='weekday',
                    y='sales',
                    text='sales',
                    height=600)
    fig8.update_layout( title_text='Weekday Sales (Troughout the year)',
                        xaxis={'categoryorder': 'total ascending'})
    # fig8.show()




    #GET ALL FILES
    figs = [fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8]


    #CREATE DIRECTORIES
    if not os.path.exists('plotly_figures_png'):
        os.mkdir('plotly_figures_png')

    if not os.path.exists('plotly_figures_jpg'):
        os.mkdir('plotly_figures_jpg')


    #SAVE IMAGES
    for i, fig in enumerate(figs):
        fig.write_image(f"plotly_figures_png/fig{i+1}.png")
        fig.write_image(f"plotly_figures_jpg/fig{i+1}.jpg")






    #EXAMPLE GRAPHS FROM PLOTLY DOCUMENTATION:
    # df55 = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')

    # fig55 = px.area(df55, x='Date', y='AAPL.High', title='Time Series with Rangeslider')

    # fig55.update_xaxes(rangeslider_visible=True)
    # # fig55.show()


    # df66 = px.data.stocks(indexed=True)-1
    # fig66 = px.area(df66, facet_col="company", facet_col_wrap=2)
    # # fig66.show()


    # df77 = px.data.stocks(indexed=True)-1
    # fig77 = px.bar(df77, x=df77.index, y="GOOG")
    # # fig77.show()



    # df88 = px.data.stocks()
    # fig88 = px.area(df88, x="date", y=df88.columns,
    #               hover_data={"date": "|%B %d, %Y"},
    #               title='custom tick labels')
    # fig88.update_xaxes(
    #     dtick="M1",
    #     tickformat="%b\n%Y")
    # # fig88.show()












#Store image paths from images I want to incluede in the report
graphs = [f'plotly_figures_jpg/fig{i+1}.jpg' for i,fig in enumerate(figs)]


#Produce static report
# interactive_report = ''
static_report = ''

for graph_url in graphs:

    _static_block = report_block_template('static', graph_url, caption='')
    # _interactive_block = report_block_template('interactive', graph_url, caption='')

    static_report += _static_block
    # interactive_report += _interactive_block


convert_html_to_pdf(static_report, 'report.pdf')