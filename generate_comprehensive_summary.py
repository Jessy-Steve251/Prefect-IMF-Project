#!/usr/bin/env python3
"""
Comprehensive Data Orchestration Summary: Apache Airflow & Prefect Integration Guide
Combines operational insights from both platforms with practical implementation details.
"""

from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, Image
from reportlab.lib import colors
from datetime import datetime

def create_summary_pdf(output_filename='Data_Orchestration_Comprehensive_Summary.pdf'):
    """Generate comprehensive PDF combining Apache Airflow and Prefect information"""
    
    # Create PDF document
    doc = SimpleDocTemplate(
        output_filename,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    
    # Container for PDF elements
    elements = []
    
    # Define styles
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    heading1_style = ParagraphStyle(
        'CustomHeading1',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#2e5c8a'),
        spaceAfter=10,
        spaceBefore=12,
        fontName='Helvetica-Bold',
        borderColor=colors.HexColor('#2e5c8a'),
        borderWidth=1,
        borderPadding=6
    )
    
    heading2_style = ParagraphStyle(
        'CustomHeading2',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=colors.HexColor('#3d7ca8'),
        spaceAfter=8,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        alignment=TA_JUSTIFY,
        spaceAfter=8,
        leading=14
    )
    
    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['BodyText'],
        fontSize=9,
        fontName='Courier',
        textColor=colors.HexColor('#333333'),
        backColor=colors.HexColor('#f5f5f5'),
        spaceAfter=6,
        leftIndent=20,
        borderColor=colors.HexColor('#cccccc'),
        borderWidth=0.5,
        borderPadding=4
    )
    
    # ============== COVER PAGE ==============
    elements.append(Spacer(1, 1.5*inch))
    elements.append(Paragraph("Data Orchestration Platform", title_style))
    elements.append(Paragraph("Comprehensive Implementation Summary", ParagraphStyle(
        'Subtitle', parent=styles['Normal'], fontSize=14, alignment=TA_CENTER, textColor=colors.HexColor('#666666')
    )))
    elements.append(Spacer(1, 0.3*inch))
    elements.append(Paragraph("Apache Airflow &amp; Prefect Integration Guide", body_style))
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph(f"<b>Document Date:</b> {datetime.now().strftime('%B %d, %Y')}", body_style))
    elements.append(Paragraph("<b>Document Version:</b> 1.0", body_style))
    elements.append(Paragraph("<b>Scope:</b> Production Deployment Configuration", body_style))
    
    elements.append(PageBreak())
    
    # ============== TABLE OF CONTENTS ==============
    elements.append(Paragraph("Table of Contents", heading1_style))
    toc_items = [
        "1. Executive Summary",
        "2. Apache Airflow Overview & Windows Platform Considerations",
        "3. Prefect Platform: Architecture & Implementation",
        "4. Comparative Analysis: Airflow vs Prefect",
        "5. Currency Exchange Rate Pipeline - Prefect Implementation",
        "6. System Configuration & Deployment",
        "7. Operational Procedures & Monitoring",
        "8. Troubleshooting & Lessons Learned",
        "9. Future Roadmap & Recommendations"
    ]
    for item in toc_items:
        elements.append(Paragraph(item, body_style))
    
    elements.append(PageBreak())
    
    # ============== SECTION 1: EXECUTIVE SUMMARY ==============
    elements.append(Paragraph("1. Executive Summary", heading1_style))
    
    elements.append(Paragraph(
        "<b>Overview:</b> This document provides a comprehensive guide for implementing data orchestration "
        "pipelines using two leading platforms: Apache Airflow and Prefect. The project focuses on building "
        "an automated currency exchange rate acquisition and processing system.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Key Outcomes:</b>",
        body_style
    ))
    outcomes = [
        "✓ Successfully implemented currency acquisition pipeline using Prefect 3.6.5",
        "✓ Hybrid execution model combining Cloud monitoring with local Windows Task Scheduler automation",
        "✓ Resolved critical infrastructure challenges (entrypoint configuration, Python path resolution)",
        "✓ Established reliable monthly execution schedule (17th of each month at 11:00, 11:30, 12:00 Europe/Zurich)",
        "✓ Production-ready configuration with comprehensive monitoring and logging"
    ]
    for outcome in outcomes:
        elements.append(Paragraph(outcome, body_style))
    
    elements.append(Spacer(1, 0.2*inch))
    
    # ============== SECTION 2: APACHE AIRFLOW ==============
    elements.append(Paragraph("2. Apache Airflow Overview &amp; Windows Platform Considerations", heading1_style))
    
    elements.append(Paragraph(
        "<b>2.1 What is Apache Airflow?</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Apache Airflow is an open-source workflow orchestration platform that allows users to define, "
        "schedule, and monitor complex workflows through Python code. It uses Directed Acyclic Graphs (DAGs) "
        "to represent data pipelines and provides a rich web UI for monitoring and management.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>2.2 Windows Compatibility Issues</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "<b>Issue 1: Unix-Specific Dependencies</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Apache Airflow depends on the <code>fcntl</code> module, which is exclusively available on Unix-based "
        "systems (Linux, macOS). This module handles file locking and Unix-specific I/O operations. Windows lacks "
        "this module, resulting in <code>ModuleNotFoundError: No module named 'fcntl'</code> errors when attempting "
        "direct installation on Windows.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Issue 2: File Path Handling Differences</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Airflow's codebase uses Unix-style file paths extensively. Windows uses backslashes (\\\\) for path "
        "separation, while Unix uses forward slashes (/). This inconsistency can cause import failures and "
        "configuration errors when running Airflow directly on Windows.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>2.3 Recommended Solution: Docker Containerization</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "The recommended approach for running Apache Airflow on Windows is through <b>Docker Desktop</b>, which "
        "provides a Linux containerization layer. This solution:",
        body_style
    ))
    
    docker_benefits = [
        "• Provides a complete Linux environment inside a container",
        "• Eliminates Windows compatibility issues entirely",
        "• Allows seamless DAG development and testing",
        "• Maintains consistency with production environments (typically Linux)",
        "• Enables easy scaling and deployment to cloud platforms"
    ]
    for benefit in docker_benefits:
        elements.append(Paragraph(benefit, body_style))
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>2.4 Docker Implementation Steps</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 1: Install Docker Desktop</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Download and install Docker Desktop from the official Docker website. After installation, "
        "ensure the Docker daemon is running.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 2: Pull and Run Airflow Container</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Execute the following command in CMD/PowerShell:",
        body_style
    ))
    elements.append(Paragraph(
        "docker run -it --rm --name airflow-devel -p 8080:8080 apache/airflow airflow standalone",
        code_style
    ))
    elements.append(Paragraph(
        "This command:<br/>"
        "• <code>-it</code>: Interactive terminal mode<br/>"
        "• <code>--rm</code>: Automatically removes container on exit<br/>"
        "• <code>-p 8080:8080</code>: Maps port 8080 for web UI access<br/>"
        "• <code>airflow standalone</code>: Runs in standalone mode (all-in-one setup)<br/>",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 3: Access the Airflow Web UI</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Once the container is running, open http://localhost:8080 in your browser to access the Airflow UI.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 4: Upload DAG Files</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Use Docker's <code>cp</code> command to transfer DAG files into the container:",
        body_style
    ))
    elements.append(Paragraph(
        "docker cp quick_test_dag.py airflow-devel:/opt/airflow/dags",
        code_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 5: Test DAG Execution</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Enter the container shell and test DAG execution:",
        body_style
    ))
    elements.append(Paragraph(
        "airflow dags test quick_test_dag",
        code_style
    ))
    
    elements.append(Paragraph(
        "<b>Step 6: View Execution Logs</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "After the DAG runs, inspect execution results and logs through the web UI or container terminal.",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== SECTION 3: PREFECT PLATFORM ==============
    elements.append(Paragraph("3. Prefect Platform: Architecture &amp; Implementation", heading1_style))
    
    elements.append(Paragraph(
        "<b>3.1 Prefect Overview</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Prefect is a modern, open-source workflow orchestration platform designed to be flexible and user-friendly. "
        "Unlike Airflow's DAG model, Prefect uses a more Pythonic approach with tasks and flows, emphasizing developer "
        "experience and observability.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Key Differences from Airflow:</b>",
        body_style
    ))
    prefect_features = [
        "• Native Python functions as tasks (no special syntax required)",
        "• Better error handling with built-in retries and timeouts",
        "• First-class cloud support (Prefect Cloud) with UI hosted in the cloud",
        "• Simpler worker management without complex broker setup",
        "• Improved task dependency management"
    ]
    for feature in prefect_features:
        elements.append(Paragraph(feature, body_style))
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>3.2 Project Implementation: Currency Exchange Rate Pipeline</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "This implementation demonstrates Prefect's capabilities with a real-world currency acquisition pipeline.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Architecture Overview:</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    
    # Architecture table
    arch_data = [
        ['Component', 'Technology', 'Purpose'],
        ['Flows', 'Python 3.11.9', 'Define data pipeline workflows'],
        ['Deployments', 'Prefect 3.6.5', 'Package and configure flows'],
        ['Schedules', 'Prefect Cloud', 'Trigger execution at specified times'],
        ['Workers', 'Windows Task Scheduler', 'Execute flows locally'],
        ['Storage', 'Local File System', 'Store pipeline data'],
        ['Monitoring', 'Prefect Cloud UI', 'View execution history and logs']
    ]
    
    arch_table = Table(arch_data, colWidths=[1.8*inch, 1.8*inch, 2*inch])
    arch_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ]))
    elements.append(arch_table)
    
    elements.append(Spacer(1, 0.2*inch))
    
    elements.append(Paragraph(
        "<b>3.3 Core Implementation Components</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "<b>Flow 1: Currency Acquisition Flow</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "<b>Purpose:</b> Fetches current exchange rates from external APIs and stores them locally.<br/>"
        "<b>Deployment:</b> currency-acquisition<br/>"
        "<b>Schedule:</b> Monthly on 17th at 11:00 Europe/Zurich<br/>"
        "<b>Output:</b> CSV file with exchange rates (exchange_rates_YYYY_MM.csv)",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Flow 2: Prepare Batch Flow</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "<b>Purpose:</b> Prepares data for batch processing by formatting and organizing exchange rate data.<br/>"
        "<b>Deployment:</b> prepare-batch<br/>"
        "<b>Schedule:</b> Monthly on 17th at 11:30 Europe/Zurich<br/>"
        "<b>Dependency:</b> Runs after currency-acquisition flow",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Flow 3: Process Batch Flow</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "<b>Purpose:</b> Processes the prepared batch data and generates final output files.<br/>"
        "<b>Deployment:</b> process-batch<br/>"
        "<b>Schedule:</b> Monthly on 17th at 12:00 Europe/Zurich<br/>"
        "<b>Dependency:</b> Runs after prepare-batch flow",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== SECTION 4: COMPARATIVE ANALYSIS ==============
    elements.append(Paragraph("4. Comparative Analysis: Airflow vs Prefect", heading1_style))
    
    comparison_data = [
        ['Aspect', 'Apache Airflow', 'Prefect'],
        ['Learning Curve', 'Moderate to Steep', 'Gentle'],
        ['Windows Support', 'Requires Docker', 'Native Support'],
        ['Execution Model', 'DAGs', 'Flows & Tasks'],
        ['Error Handling', 'Basic retry mechanism', 'Advanced with better defaults'],
        ['Cloud Solution', 'Third-party (Astronomer)', 'Prefect Cloud (built-in)'],
        ['Dependency Management', 'Complex (Celery/RabbitMQ)', 'Simplified with Workers'],
        ['Development Experience', 'Good', 'Excellent'],
        ['Monitoring UI', 'Self-hosted', 'Cloud-hosted (free tier available)'],
        ['Cost', 'Free (self-hosted)', 'Free tier + paid plans'],
        ['Production Readiness', 'Very High', 'Very High']
    ]
    
    comparison_table = Table(comparison_data, colWidths=[1.5*inch, 2*inch, 2*inch])
    comparison_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ]))
    elements.append(comparison_table)
    
    elements.append(Spacer(1, 0.2*inch))
    
    elements.append(Paragraph(
        "<b>4.1 Selection Rationale</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "For this project, <b>Prefect was selected over Apache Airflow</b> for the following reasons:<br/>"
        "1. <b>Native Windows Compatibility:</b> No Docker required for local development and testing<br/>"
        "2. <b>Simpler Setup:</b> Minimal configuration required to get started<br/>"
        "3. <b>Better Developer Experience:</b> Pythonic API makes development faster<br/>"
        "4. <b>Cloud-Ready:</b> Prefect Cloud integration provides monitoring without additional infrastructure<br/>"
        "5. <b>Cost Efficiency:</b> Free tier suitable for this use case",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== SECTION 5: CURRENCY PIPELINE IMPLEMENTATION ==============
    elements.append(Paragraph("5. Currency Exchange Rate Pipeline - Prefect Implementation", heading1_style))
    
    elements.append(Paragraph(
        "<b>5.1 System Architecture</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "The implementation uses a hybrid execution model combining cloud monitoring with local execution:",
        body_style
    ))
    
    arch_diagram = [
        ['Layer', 'Component', 'Status', 'Purpose'],
        ['Cloud', 'Prefect Cloud Schedules', 'Active (monitoring)', 'Track execution history'],
        ['Cloud', 'Prefect Cloud UI', 'Active', 'View flow execution logs'],
        ['Local', 'Windows Task Scheduler', 'Active (execution)', 'Trigger flows at scheduled times'],
        ['Local', 'Python Interpreter', 'Active', 'Execute flow code'],
        ['Local', 'Data Storage', 'Active', 'Store exchange rate data']
    ]
    
    arch_diag_table = Table(arch_diagram, colWidths=[1.2*inch, 1.8*inch, 1.5*inch, 1.8*inch])
    arch_diag_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ]))
    elements.append(arch_diag_table)
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>5.2 Critical Implementation Decisions</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "<b>Decision 1: Hybrid Execution Model (Cloud + Local)</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Initially attempted to run flows directly in Prefect Cloud, but encountered a limitation: "
        "Cloud's <code>prefect:managed</code> work pool cannot access local code files stored on the user's computer. "
        "Solution: Configure Windows Task Scheduler to trigger local Python execution while maintaining Cloud monitoring. "
        "This provides the best of both worlds: reliable execution + centralized logging.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Decision 2: Windows Task Scheduler Over Prefect Cloud Scheduler</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "While Prefect Cloud provides scheduling capabilities, the local execution model necessitates Windows Task Scheduler "
        "as the actual trigger mechanism. Cloud schedules are maintained for monitoring and historical record-keeping.",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>Decision 3: Absolute Python Path in Batch Files</b>",
        ParagraphStyle('SubSubHeading', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#3d7ca8'), fontName='Helvetica-Bold')
    ))
    elements.append(Paragraph(
        "Critical fix: All batch files must use the absolute Python path "
        "(<code>C:\\Program Files\\Python311\\python.exe</code>) rather than relative <code>python</code> command. "
        "Task Scheduler doesn't inherit the system PATH, causing execution failures with relative paths.",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== SECTION 6: SYSTEM CONFIGURATION ==============
    elements.append(Paragraph("6. System Configuration &amp; Deployment", heading1_style))
    
    elements.append(Paragraph(
        "<b>6.1 Environment Specifications</b>",
        heading2_style
    ))
    
    env_data = [
        ['Component', 'Value'],
        ['Operating System', 'Windows 11'],
        ['Python Version', '3.11.9'],
        ['Prefect Version', '3.6.5'],
        ['Timezone', 'Europe/Zurich (UTC+1/+2)'],
        ['Work Pool', 'Yichen_Test (prefect:managed)']
    ]
    
    env_table = Table(env_data, colWidths=[2.5*inch, 3.5*inch])
    env_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ]))
    elements.append(env_table)
    
    elements.append(Spacer(1, 0.2*inch))
    
    elements.append(Paragraph(
        "<b>6.2 Deployment Configuration</b>",
        heading2_style
    ))
    
    deploy_data = [
        ['Deployment', 'Entrypoint', 'Schedule', 'Timezone'],
        ['currency-acquisition', 'flows/currency_acquisition_flow.py:<br/>currency_acquisition_flow', 
         'Monthly 17th<br/>11:00', 'Europe/Zurich'],
        ['prepare-batch', 'flows/prepare_batch_flow.py:<br/>prepare_batch_flow',
         'Monthly 17th<br/>11:30', 'Europe/Zurich'],
        ['process-batch', 'flows/process_batch_flow.py:<br/>process_batch_flow',
         'Monthly 17th<br/>12:00', 'Europe/Zurich']
    ]
    
    deploy_table = Table(deploy_data, colWidths=[1.4*inch, 1.8*inch, 1.4*inch, 1.4*inch])
    deploy_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ]))
    elements.append(deploy_table)
    
    elements.append(Spacer(1, 0.2*inch))
    
    elements.append(Paragraph(
        "<b>6.3 Configuration Files Overview</b>",
        heading2_style
    ))
    
    config_files = [
        ('prefect.yaml', 'Defines deployments with entrypoint specifications'),
        ('run_flows_locally.py', 'Local execution wrapper that imports and runs all flows'),
        ('run_Prefect-*.bat (3 files)', 'Batch files triggered by Task Scheduler with full Python path'),
        ('requirements.txt', 'Python dependencies (Prefect, pandas, etc.)'),
        ('check_status.py', 'Verification script to check deployment and schedule status')
    ]
    
    for filename, description in config_files:
        elements.append(Paragraph(f"<b>{filename}:</b> {description}", body_style))
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>6.4 Key Configuration Parameters</b>",
        heading2_style
    ))
    
    elements.append(Paragraph(
        "<b>Python Installation Path:</b> C:\\\\Program Files\\\\Python311\\\\python.exe<br/>"
        "<b>Project Root:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project<br/>"
        "<b>Data Output Directory:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\data\\\\<br/>"
        "<b>Flows Directory:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\flows\\\\<br/>"
        "<b>Utilities Directory:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\utils\\\\",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== SECTION 7: OPERATIONAL PROCEDURES ==============
    elements.append(Paragraph("7. Operational Procedures &amp; Monitoring", heading1_style))
    
    elements.append(Paragraph(
        "<b>7.1 Daily Operational Checks</b>",
        heading2_style
    ))
    
    checks = [
        ('Prefect Cloud UI Status', 'Verify that Cloud is accessible and showing current deployment status'),
        ('Windows Task Scheduler', 'Confirm all 3 tasks are enabled and showing correct schedule'),
        ('Data Directory', 'Check for presence of latest exchange rate files'),
        ('System Logs', 'Review Windows Event Viewer for any Task Scheduler errors')
    ]
    
    for check_name, check_desc in checks:
        elements.append(Paragraph(f"<b>{check_name}:</b> {check_desc}", body_style))
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>7.2 Monitoring Dashboard</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Access Prefect Cloud at https://app.prefect.cloud to view:<br/>"
        "• Real-time flow execution status<br/>"
        "• Historical execution logs and run times<br/>"
        "• Performance metrics and error tracking<br/>"
        "• Schedule status and next scheduled execution times",
        body_style
    ))
    
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph(
        "<b>7.3 Verification Commands</b>",
        heading2_style
    ))
    
    commands = [
        ('Check Current Status', 'python check_status.py', 
         'Displays deployment status, schedule information, and configuration'),
        ('Manual Flow Execution', 'python run_flows_locally.py',
         'Manually triggers all three flows for testing purposes'),
        ('Task Scheduler Query', 'schtasks /query | findstr Prefect',
         'Lists all Prefect-related tasks in Windows Task Scheduler'),
        ('Create Deployment', 'prefect deployment create flows/currency_acquisition_flow.py:currency_acquisition_flow --name currency-acquisition',
         'Creates a new deployment in Prefect'),
        ('Create Cloud Schedule', 'prefect deployment schedule create [deployment/name] --cron "0 11 17 * *" --timezone "Europe/Zurich"',
         'Creates a monthly schedule at 11:00 on the 17th')
    ]
    
    for cmd_name, cmd_text, cmd_desc in commands:
        elements.append(Paragraph(f"<b>{cmd_name}:</b>", body_style))
        elements.append(Paragraph(cmd_text, code_style))
        elements.append(Paragraph(cmd_desc, ParagraphStyle('Note', parent=styles['Normal'], fontSize=9, 
                                                           textColor=colors.HexColor('#666666'), 
                                                           leftIndent=20, spaceAfter=8)))
    
    elements.append(PageBreak())
    
    # ============== SECTION 8: TROUBLESHOOTING ==============
    elements.append(Paragraph("8. Troubleshooting &amp; Lessons Learned", heading1_style))
    
    elements.append(Paragraph(
        "<b>8.1 Common Issues &amp; Solutions</b>",
        heading2_style
    ))
    
    issues = [
        {
            'title': 'Issue 1: "Deployment does not have an entrypoint"',
            'cause': 'Deployment created without specifying entrypoint parameter',
            'solution': 'Recreate deployment with full entrypoint: flows/flow_name.py:flow_name'
        },
        {
            'title': 'Issue 2: Task Scheduler returns exit code -1073741510',
            'cause': 'Batch file uses relative python path which is not in Task Scheduler PATH',
            'solution': 'Use absolute Python path: C:\\\\Program Files\\\\Python311\\\\python.exe'
        },
        {
            'title': 'Issue 3: "ModuleNotFoundError: No module named fcntl" (if using Airflow)',
            'cause': 'fcntl is Unix-specific and not available on Windows',
            'solution': 'Use Docker Desktop to run Airflow in a Linux container'
        },
        {
            'title': 'Issue 4: Cloud Schedules not executing',
            'cause': 'Cloud prefect:managed cannot access local code files',
            'solution': 'Use local execution model with Windows Task Scheduler'
        },
        {
            'title': 'Issue 5: Cloud UI shows old schedule times',
            'cause': 'Schedules updated locally but Cloud configuration not synchronized',
            'solution': 'Delete old Cloud schedules and create new ones with correct cron expressions'
        }
    ]
    
    for issue in issues:
        elements.append(Paragraph(f"<b>{issue['title']}</b>", 
                                 ParagraphStyle('IssueTitle', parent=styles['Normal'], fontSize=11, 
                                              textColor=colors.HexColor('#8B0000'), fontName='Helvetica-Bold')))
        elements.append(Paragraph(f"<b>Root Cause:</b> {issue['cause']}", body_style))
        elements.append(Paragraph(f"<b>Solution:</b> {issue['solution']}", body_style))
        elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph(
        "<b>8.2 Lessons Learned</b>",
        heading2_style
    ))
    
    lessons = [
        "1. <b>Always use absolute paths in batch files</b> when using Task Scheduler, as relative paths may not resolve correctly",
        "2. <b>Synchronize configuration across systems</b> - when updating schedules, update both Cloud and local configs to maintain consistency",
        "3. <b>Docker is essential for Airflow on Windows</b> - native Windows support is unreliable due to Unix-specific dependencies",
        "4. <b>Prefect's local execution is more flexible</b> than Cloud execution for scenarios with local code and data",
        "5. <b>Test thoroughly before production</b> - manual execution tests revealed issues that wouldn't surface in dry runs",
        "6. <b>Monitor both Cloud and local execution</b> - maintain awareness of both systems for complete troubleshooting visibility",
        "7. <b>Document configuration changes</b> - track all schedule and deployment changes for audit and recovery purposes"
    ]
    
    for lesson in lessons:
        elements.append(Paragraph(lesson, body_style))
    
    elements.append(PageBreak())
    
    # ============== SECTION 9: FUTURE ROADMAP ==============
    elements.append(Paragraph("9. Future Roadmap &amp; Recommendations", heading1_style))
    
    elements.append(Paragraph(
        "<b>9.1 Production Configuration Update</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Current test configuration (monthly 17th) should be updated to production schedule once validated. "
        "Production dates: 15th, 25th, 28th-31st of each month at 11:00, 11:30, 12:00 (Europe/Zurich).",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>9.2 Enhanced Monitoring</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Recommendations for monitoring improvements:<br/>"
        "• Implement email notifications for failed executions<br/>"
        "• Set up Slack integration for real-time alerts<br/>"
        "• Create custom dashboards for data quality metrics<br/>"
        "• Implement automated backup verification",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>9.3 Scalability Considerations</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "To scale this system:<br/>"
        "• <b>Migrate to Cloud Execution:</b> Refactor to use Cloud-compatible code structure<br/>"
        "• <b>Implement Database Storage:</b> Move from CSV files to database for better querying<br/>"
        "• <b>Add Data Validation:</b> Implement automated data quality checks<br/>"
        "• <b>Enable Concurrent Flows:</b> Modify dependencies to allow parallel execution where possible<br/>"
        "• <b>Implement Versioning:</b> Track flow version history and rollback capabilities",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>9.4 Technology Evolution Path</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "Short-term (0-6 months):<br/>"
        "• Finalize production configuration and validate monthly execution<br/>"
        "• Implement comprehensive monitoring and alerting<br/>"
        "• Document runbooks for common operations<br/>"
        "<br/>"
        "Medium-term (6-12 months):<br/>"
        "• Evaluate cloud migration (Azure, AWS, GCP)<br/>"
        "• Implement data warehouse for historical analysis<br/>"
        "• Explore Prefect Cloud's advanced features<br/>"
        "<br/>"
        "Long-term (12+ months):<br/>"
        "• Consider transitioning to fully cloud-native architecture<br/>"
        "• Integrate with business intelligence tools<br/>"
        "• Expand to multi-region deployment for redundancy",
        body_style
    ))
    
    elements.append(PageBreak())
    
    # ============== APPENDIX ==============
    elements.append(Paragraph("Appendix: Quick Reference Guide", heading1_style))
    
    elements.append(Paragraph(
        "<b>A.1 Critical File Paths</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "<b>Python Executable:</b> C:\\\\Program Files\\\\Python311\\\\python.exe<br/>"
        "<b>Project Root:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project<br/>"
        "<b>Flow Files:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\flows\\\\<br/>"
        "<b>Data Output:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\data\\\\<br/>"
        "<b>Batch Files:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\run_Prefect-*.bat",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>A.2 Essential URLs</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "<b>Prefect Cloud:</b> https://app.prefect.cloud<br/>"
        "<b>Prefect Documentation:</b> https://docs.prefect.io<br/>"
        "<b>Apache Airflow:</b> https://airflow.apache.org<br/>"
        "<b>Airflow Documentation:</b> https://airflow.apache.org/docs/apache-airflow",
        body_style
    ))
    
    elements.append(Paragraph(
        "<b>A.3 Support Contact Information</b>",
        heading2_style
    ))
    elements.append(Paragraph(
        "<b>Project Owner:</b> Yichen Li<br/>"
        "<b>Prefect Cloud Account:</b> Associated with yli@intracen.com<br/>"
        "<b>Documentation Location:</b> C:\\\\Users\\\\yli\\\\Desktop\\\\Prefect_Project\\\\*.md",
        body_style
    ))
    
    elements.append(Spacer(1, 0.3*inch))
    
    elements.append(Paragraph(
        "<b>Document Information</b>",
        ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, alignment=TA_CENTER, 
                      textColor=colors.HexColor('#666666'))
    ))
    elements.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}<br/>"
        "Version: 1.0<br/>"
        "Classification: Technical Documentation",
        ParagraphStyle('FooterText', parent=styles['Normal'], fontSize=8, alignment=TA_CENTER, 
                      textColor=colors.HexColor('#999999'))
    ))
    
    # Build PDF
    doc.build(elements)
    print(f"✓ PDF generated successfully: {output_filename}")
    return output_filename

if __name__ == '__main__':
    pdf_file = create_summary_pdf()
    print(f"✓ Complete summary available at: {pdf_file}")
