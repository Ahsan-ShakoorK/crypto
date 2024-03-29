Crypto dashboard for anaylzing 

1.	Access market trade data from the database found at https://www.cryptocompare.com/cryptopian/api-keys using the supplied API key.
2.	Utilize this data to construct a web application that exhibits cryptocurrency market trading data.
3.	The application should visually represent the amount of coins traded at various unique price points.
4.	Generate a table with six columns, the number of rows being determined by the unique price points in the obtained data.
5.	The columns should correspond to the following:
•	Column 1: Display unique prices at which trades occurred in the past 1 hours. Summarize multiple instances of the same price into one row. Each row should also include the specific price at which coins were traded.
•	Column 2: Depict the number of coins traded at each unique price in the last 1 minute.
•	Columns 3, 4, and 5: Indicate the number of coins traded at each unique price during the last 5, 15, and 60 minutes, respectively. Note:
•	The data in the 5-minute column should include trades from 0 to 5 minutes ago.
•	The 15-minute column should cover trades from 5 to 15 minutes ago.
•	The 60-minute column should contain trades from 15 to 60 minutes ago.

6.	Ensure the table updates every 15 seconds with new data.
7.	Embed a search box in the application, enabling users to search for a specific cryptocurrency paired with USDT. For example, entering "et" should suggest "ethusdt".
8.	The search box should fetch the list of coin names from the API, offering coin name suggestions to the user in real-time.
9.	Develop the web application using Python and Streamlit.
10.	Retrieve the data in a manner that permits computations on all potential data points. If feasible, include data in as small as 5-second intervals, enabling thorough and accurate data analysis.
11.	The resultant code should ensure the web application possesses a user-friendly interface, designed to fetch and display all possible data provided by the API, thereby maximizing the amount of information available to the user.

