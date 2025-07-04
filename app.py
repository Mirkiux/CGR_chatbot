import streamlit as st
from snowflake.core import Root  # requires snowflake>=0.8.0
from snowflake.snowpark.context import get_active_session

MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# PROMPTS ADAPTADOS
PROMPT_MAIN = """
[INST]
Usted es un asistente de inteligencia artificial diseñado para responder preguntas utilizando capacidades de recuperación aumentada por contexto (RAG). Cuando se le formule una pregunta, se le proporcionará también un contexto entre las etiquetas <context> y </context>, y el historial de la conversación entre <chat_history> y </chat_history>. Utilice la información proporcionada en el contexto y el historial para entregar una respuesta clara, concisa, directamente relevante y en lenguaje formal.

El contexto aumentado por recuperación, se refiere a informes de control realizados por la Contraloría General de la República del Perú(CGR) a funcionarios públicos o entidades estatales, con base en indicios de posibles delitos de varios tipos y dan una conslusión de si esos indicios ameritan ciertas acciones correctivas o penales, una investigación más profunda o sólo recomendaciones básicas a la persona o entidad.

Los indicios pueden provenir de investigaciones de oficio realizadas por la CGR, o de denuncias que se recibieron de ciudadanos, las cuales pueden ser anónimas o no.

Al responder, cite siempre el número de documento (recurd_number)  de donde extrajo la información relevante.


Si la pregunta no se puede responder con el contexto ni el historial proporcionados, indique: “No dispongo de información suficiente para responder a esa pregunta.”

No indique frases como “según el contexto proporcionado” ni haga referencia al sistema.

<chat_history>
{chat_history}
</chat_history>
<context>
{prompt_context}
</context>
<question>
{user_question}
</question>
[/INST]
Respuesta:

Advertencia: Esta respuesta es generada automáticamente y no constituye una interpretación legal oficial.
"""

PROMPT_SUMMARY = """
[INST]
Con base en el siguiente historial de la conversación y la pregunta actual, genere una versión ampliada de la pregunta que integre el contexto relevante del historial. Responda únicamente con la pregunta ampliada en lenguaje formal, sin añadir explicaciones.

<chat_history>
{chat_history}
</chat_history>
<question>
{question}
</question>
[/INST]
"""

OUT_OF_SCOPE_MSG = """\
Lo siento, solo puedo responder preguntas relacionadas con informes de control.
Por favor, formule una consulta referida a un informe o sección específica.

Advertencia: Esta respuesta es generada automáticamente y no constituye una interpretación legal oficial.
"""

# --- FUNCIONES ---

def init_messages():
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def init_service_metadata():
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                svc_search_col = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {svc_name};"
                ).collect()[0]["search_column"]
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )
        st.session_state.service_metadata = service_metadata

def init_config_options():
    st.sidebar.selectbox(
        "Seleccione el servicio de búsqueda Cortex:",
        [s["name"] for s in st.session_state.service_metadata],
        index=[s["name"] for s in st.session_state.service_metadata].index("INFORMES_DATASET_SERVICE"),
        key="selected_cortex_search_service",
    )
    st.sidebar.button("Limpiar conversación", key="clear_conversation")
    st.sidebar.toggle("Modo debug", key="debug", value=False)
    st.sidebar.toggle("Utilizar historial de chat", key="use_chat_history", value=True)

    with st.sidebar.expander("Opciones avanzadas"):
        st.selectbox("Seleccione modelo:", MODELS, key="model_name")
        st.number_input(
            "Cantidad de fragmentos de contexto",
            value=5,
            key="num_retrieved_chunks",
            min_value=1,
            max_value=10,
        )
        st.number_input(
            "Cantidad de mensajes en historial de chat",
            value=5,
            key="num_chat_messages",
            min_value=1,
            max_value=10,
        )

    st.sidebar.expander("Estado de la sesión").write(st.session_state)

def query_cortex_search_service(query):
    db, schema = "CGR_INFORMES", "PARSING_INFORMES"  # Cambiado aquí según instrucción
    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services[st.session_state.selected_cortex_search_service]
    )

    context_documents = cortex_search_service.search(
        query, columns=[], limit=st.session_state.num_retrieved_chunks
    )
    results = context_documents.results

    service_metadata = st.session_state.service_metadata
    search_col = [s["search_column"] for s in service_metadata
                    if s["name"] == st.session_state.selected_cortex_search_service][0]

    context_str = ""
    for i, r in enumerate(results):
        # Cita siempre relative_path y section_id si existen en r
        relative_path = r.get('RELATIVE_PATH', 'N/D')
        section_id = r.get('SECTION_ID', 'N/D')
        texto = r[search_col]
        context_str += (
            f"Documento: {relative_path}, Sección: {section_id}\n"
            f"{texto}\n\n"
        )

    if st.session_state.debug:
        st.sidebar.text_area("Documentos de contexto", context_str, height=500)

    return context_str

def get_chat_history():
    start_index = max(
        0, len(st.session_state.messages) - st.session_state.num_chat_messages
    )
    # Usar solo los mensajes previos al último (el último es la pregunta actual)
    return st.session_state.messages[start_index : len(st.session_state.messages) - 1]

def complete(model, prompt):
    return session.sql("SELECT snowflake.cortex.complete(?,?)", (model, prompt)).collect()[0][0]

def make_chat_history_summary(chat_history, question):
    chat_history_str = ""
    for msg in chat_history:
        chat_history_str += f"{msg['role'].capitalize()}: {msg['content']}\n"
    prompt = PROMPT_SUMMARY.format(chat_history=chat_history_str, question=question)
    summary = complete(st.session_state.model_name, prompt)

    if st.session_state.debug:
        st.sidebar.text_area(
            "Resumen de historial de chat", summary.replace("$", "\$"), height=150
        )

    return summary

def create_prompt(user_question):
    # Chat history
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        chat_history_str = ""
        if chat_history != []:
            for msg in chat_history:
                chat_history_str += f"{msg['role'].capitalize()}: {msg['content']}\n"
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context = query_cortex_search_service(question_summary)
        else:
            prompt_context = query_cortex_search_service(user_question)
            chat_history_str = ""
    else:
        prompt_context = query_cortex_search_service(user_question)
        chat_history_str = ""

    prompt = PROMPT_MAIN.format(
        chat_history=chat_history_str,
        prompt_context=prompt_context,
        user_question=user_question
    )
    return prompt

def pregunta_fuera_de_tema(question):
    # Regla simple: buscar palabras clave asociadas a informes y secciones
    keywords = [
        "informe", "control", "contraloría", "sección", "section", "relativo", "auditoría", 
        "documento", "relative_path", "section_id", "observación", "hallazgo", "recomendación", "entidad"
    ]
    question_lc = question.lower()
    return not any(k in question_lc for k in keywords)

def main():
    st.title(f":speech_balloon: Chatbot - Informes de Control")

    init_service_metadata()
    init_config_options()
    init_messages()

    icons = {"assistant": "❄️", "user": "👤"}

    # Mostrar mensajes históricos
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            st.markdown(message["content"])

    disable_chat = (
        "service_metadata" not in st.session_state
        or len(st.session_state.service_metadata) == 0
    )
    if question := st.chat_input("Ingrese su consulta...", disabled=disable_chat):
        # Añadir mensaje del usuario al historial
        st.session_state.messages.append({"role": "user", "content": question})
        # Mostrar mensaje usuario
        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question.replace("$", "\$"))

        # Mostrar respuesta del asistente
        with st.chat_message("assistant", avatar=icons["assistant"]):
            message_placeholder = st.empty()
            question_clean = question.replace("'", "")
            with st.spinner("Pensando..."):
                # Filtrado fuera de tema
                if pregunta_fuera_de_tema(question_clean):
                    generated_response = OUT_OF_SCOPE_MSG
                else:
                    generated_response = complete(
                        st.session_state.model_name, create_prompt(question_clean)
                    )
                message_placeholder.markdown(generated_response)

        st.session_state.messages.append(
            {"role": "assistant", "content": generated_response}
        )

if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()
