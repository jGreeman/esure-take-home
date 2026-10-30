# dataengineering-takehometest

## Data Engineering Take Home Test

### The Task

Your task is to write an ETL program to read json data and perform a series of transformations on it to create a data model.
Additionally, provide documentation on solution design, instructions for execution, assumptions, explanation of the scope and describe what additional steps, considerations, or improvements you would make to scale this solution and integrate it into a larger architecture in a production environment.

## Context:

- Provided to you in the `policy_data` folder is files in json format containing fake policy events data. This data needs to be converted into a usable and queryable format for donwstream reporting and analysis.

Just to give you a flavour of the sort of questions the analyst teams might get asked, we've included some below. Please don't consider these to be exhaustive, more illustrative of the typical usecases which could help illuminate the types of queries we might expect on the datasets.

- How many customers currently have active policies?
- How many renewals were be sent in 2024 split by the policy type?
- What is the median renewal price in auto by day?

## Guide & Tips:

- The team is interested in the approach you follow for solving this problem, so approach it as a green-fields project and build an end to end ETL pipeline which is robust and scaleable.
- Detail how you would test the system, and what would be required before it could be moved into production
- The preferred language is **Python**
- Think about the execution environment and how portable your solution is: how would it handle and recover from failure, and how it would scale to meet large variations in the incoming size of the datasets
- How would you model the data to reduce redundancy and make querying as efficient as possible.
- We have provided some scope but if you find that it’s too broad, you are welcome to limit the scope, just let us know
  the thought process and assumptions behind your decisions.
- Do not forget to add instructions for the evaluation team on how to read and execute your code
- Use of AI tools: The use of AI-assisted development tools is not discouraged; however, you will be expected to walk through your solution with the team. You should therefore be able to clearly explain how your code works, how it has been structured, and the rationale behind any design decisions and trade-offs made.
