import streamlit as st
import base64
import json
from src.misc import get_username_from_header
from src.bedrock import answer_query 
from src.tools import check_for_tool_use, tool_use

DATABASE_NAME = 'amazon_security_lake_glue_db_ap_southeast_2'

if 'given_name' not in st.session_state:
    st.session_state.given_name = get_username_from_header()

st.title(f"Ask Fred")
st.subheader(f"about security and infra")

if st.session_state.given_name:
    st.write(f"Hello, {st.session_state.given_name}. I'm here to help")
else:
    st.write("Unable to retrieve username.")

if 'messages' not in st.session_state:
    st.session_state.messages = []
    
for message in st.session_state.messages[-10:]:
    try:
        test = message["content"][0]["text"]
        with st.chat_message(message["role"]):
            st.markdown(message["content"][0]["text"])
    except:
        pass

question = st.chat_input('what would you like to know? Enter !learn to learn the schema')
if question:
    with st.chat_message("user"):
        st.markdown(question)
        if question == '!learn':
            question = f"Hey Claude, what tables are in the database named {DATABASE_NAME} ?. After discovering the tables, get the schema for each table \
            by running a query like 'SELECT * FROM tablename LIMIT 3'. Include the database name in all querys" 
        st.session_state.messages.append({
            "role": "user",
            "content": [
                { "text": question }
            ]
        })
    with st.chat_message("assistant"):
        
        status = st.status("Sending request to LLM....", expanded=False, state="running")
        answer = answer_query(st.session_state.messages)
        status.update(label='Done', expanded=False, state="complete")    
        st.session_state.messages.append(answer)
        st.markdown(answer['content'][0]['text'])
        
        tooluse = tool_use(st.session_state.messages[-1]["content"])
        if tooluse["toolUse"] is True:
            status.update(label=f"The LLM asked to use a function tool, {tooluse['info']}. Procesing and sending to LLM", expanded=False, state="running")
        else:
            status.update(label=f"No futher tool use", expanded=False, state="complete")
        
        follow_up_blocks = check_for_tool_use(st.session_state.messages[-1]["content"])
        
        while len(follow_up_blocks) > 0:
            follow_up_message = {
                "role": "user",
                "content": follow_up_blocks,
            }
            st.session_state.messages.append(follow_up_message)

            answer = answer_query(st.session_state.messages)
            status.update(label='The LLM got the results of the tool, now you have to wait', expanded=False, state="running")
            
            st.session_state.messages.append(answer)
            try:
                st.markdown(answer["content"][0]["text"])
            except:
                pass
            follow_up_blocks = check_for_tool_use(st.session_state.messages[-1]["content"])
            if len(follow_up_blocks) == 0:
                status.update(label='This is the final answer from the LLM', expanded=False, state="complete")

