#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate comprehensive Prefect operation guide PDF
This script creates a detailed PDF manual with all operation instructions
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, 
    TableStyle
)
from reportlab.lib import colors
from datetime import datetime
import os


class PDFGenerator:
    """Generate comprehensive operational guide PDF"""
    
    def __init__(self, output_filename="Prefect_Operation_Manual.pdf"):
        self.output_filename = output_filename
        self.doc = SimpleDocTemplate(
            output_filename,
            pagesize=letter,
            rightMargin=0.5*inch,
            leftMargin=0.5*inch,
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
            title="Prefect Cloud Operation Manual"
        )
        self.styles = getSampleStyleSheet()
        self._define_custom_styles()
        self.story = []
        
    def _define_custom_styles(self):
        """Define custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1f4788'),
            spaceAfter=12,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomHeading',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#2563eb'),
            spaceAfter=8,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomSubheading',
            parent=self.styles['Heading3'],
            fontSize=11,
            textColor=colors.HexColor('#1e40af'),
            spaceAfter=6,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomBody',
            parent=self.styles['BodyText'],
            fontSize=9,
            alignment=TA_JUSTIFY,
            spaceAfter=4,
            leading=12
        ))
        
        self.styles.add(ParagraphStyle(
            name='CodeStyle',
            parent=self.styles['Normal'],
            fontSize=8,
            fontName='Courier',
            textColor=colors.HexColor('#2d3748'),
            leftIndent=15,
            spaceAfter=4,
            backColor=colors.HexColor('#f5f5f5')
        ))
        
    def add_title_page(self):
        """Add title page"""
        self.story.append(Spacer(1, 1.5*inch))
        
        # Main title
        title = Paragraph(
            "Prefect Cloud<br/>Operation Manual",
            self.styles['CustomTitle']
        )
        self.story.append(title)
        
        self.story.append(Spacer(1, 0.3*inch))
        
        # Subtitle
        subtitle = Paragraph(
            "Automated Exchange Rate Acquisition Pipeline<br/>Complete Setup & Operation Guide",
            self.styles['Heading2']
        )
        self.story.append(subtitle)
        
        self.story.append(Spacer(1, 0.5*inch))
        
        # Date and version
        info_data = [
            ['Generated:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ['Version:', '1.0'],
            ['Project:', 'Exchange Rate Pipeline'],
            ['Platform:', 'Prefect Cloud 3.6.5'],
            ['Status:', 'Production Ready']
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 3*inch])
        info_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        
        self.story.append(info_table)
        self.story.append(PageBreak())
        
    def add_table_of_contents(self):
        """Add table of contents"""
        self.story.append(Paragraph("Table of Contents", self.styles['CustomHeading']))
        self.story.append(Spacer(1, 0.2*inch))
        
        toc_items = [
            ("1. System Overview", "Quick summary and architecture"),
            ("2. Initial Setup", "Environment configuration and deployment"),
            ("3. Schedule Configuration", "Setting up automatic execution"),
            ("4. Worker Management", "Starting and managing Worker process"),
            ("5. Daily Operations", "Monitoring and maintenance"),
            ("6. Automatic Execution Guide", "How automatic execution works"),
            ("7. Troubleshooting", "Common issues and solutions"),
            ("8. API Documentation", "Exchange rate fetcher details"),
            ("9. Support & Contacts", "Getting help and support"),
            ("10. Appendix", "Reference tables and commands"),
        ]
        
        for item, desc in toc_items:
            line = Paragraph(f"<b>{item}</b> — {desc}", self.styles['CustomBody'])
            self.story.append(line)
            self.story.append(Spacer(1, 0.1*inch))
        
        self.story.append(PageBreak())
        
    def add_section_1_overview(self):
        """Add system overview section"""
        self.story.append(Paragraph("1. System Overview", self.styles['CustomHeading']))
        
        # Quick summary
        self.story.append(Paragraph("Quick Summary", self.styles['CustomSubheading']))
        summary = "This system automatically fetches monthly exchange rates from the International Monetary Fund (IMF) API and processes the data. It runs on Prefect Cloud with three interconnected workflows scheduled to execute on specific dates each month (15th, 25th, 28-31st)."
        self.story.append(Paragraph(summary, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Key metrics
        self.story.append(Paragraph("Key Metrics", self.styles['CustomSubheading']))
        metrics_data = [
            ['Metric', 'Value'],
            ['Countries Supported', '118'],
            ['Currencies Tracked', '77'],
            ['Execution Frequency', 'Monthly (6+ dates/month)'],
            ['Average Cycle Time', '~65 seconds'],
            ['Uptime Target', '99.9%'],
            ['Data Format', 'CSV'],
            ['Cloud Platform', 'Prefect Cloud 3.6.5'],
        ]
        
        metrics_table = Table(metrics_data, colWidths=[2*inch, 2.5*inch])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2563eb')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        self.story.append(metrics_table)
        self.story.append(Spacer(1, 0.15*inch))
        
        # Architecture diagram (text-based)
        self.story.append(Paragraph("System Architecture", self.styles['CustomSubheading']))
        arch = "Prefect Cloud (Scheduler) → Your Computer (Worker) → APIs (IMF, REST Countries)<br/><br/>Flow 1: currency-acquisition at 09:00 - Fetches FX rates<br/>Flow 2: prepare-batch at 09:30 - Generates manifests<br/>Flow 3: process-batch at 10:00 - Archives data"
        self.story.append(Paragraph(arch, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_2_setup(self):
        """Add initial setup section"""
        self.story.append(Paragraph("2. Initial Setup", self.styles['CustomHeading']))
        
        # Environment setup
        self.story.append(Paragraph("Step 1: Verify Python Environment", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Check Python version (must be 3.11+):",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "python --version",
            self.styles['CodeStyle']
        ))
        self.story.append(Spacer(1, 0.1*inch))
        
        # Install dependencies
        self.story.append(Paragraph("Step 2: Install Dependencies", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Install required packages:",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "pip install -r requirements.txt",
            self.styles['CodeStyle']
        ))
        self.story.append(Spacer(1, 0.1*inch))
        
        # Cloud login
        self.story.append(Paragraph("Step 3: Login to Prefect Cloud", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Authenticate with Prefect Cloud:",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "prefect cloud login",
            self.styles['CodeStyle']
        ))
        self.story.append(Spacer(1, 0.1*inch))
        
        # Deploy
        self.story.append(Paragraph("Step 4: Deploy Flows", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Deploy all three flows to Prefect Cloud:",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "prefect deploy",
            self.styles['CodeStyle']
        ))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Verification
        self.story.append(Paragraph("Verification", self.styles['CustomSubheading']))
        verify_steps = "✓ All 3 deployments visible in Cloud UI<br/>✓ Deployments assigned to 'Yichen_Test' pool<br/>✓ No errors in deployment output"
        self.story.append(Paragraph(verify_steps, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_3_scheduling(self):
        """Add scheduling section"""
        self.story.append(Paragraph("3. Schedule Configuration", self.styles['CustomHeading']))
        
        self.story.append(Paragraph(
            "Configure automatic monthly execution schedules in Prefect Cloud UI.",
            self.styles['CustomBody']
        ))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Cron expressions
        self.story.append(Paragraph("Cron Expressions by Flow", self.styles['CustomSubheading']))
        
        cron_data = [
            ['Flow', 'Time', 'Cron Expression', 'Dates'],
            ['currency-acquisition', '09:00', '0 9 15,25,28,29,30,31 * *', '15, 25, 28-31'],
            ['prepare-batch', '09:30', '30 9 15,25,28,29,30,31 * *', '15, 25, 28-31'],
            ['process-batch', '10:00', '0 10 15,25,28,29,30,31 * *', '15, 25, 28-31'],
        ]
        
        cron_table = Table(cron_data, colWidths=[1.3*inch, 0.8*inch, 1.8*inch, 0.8*inch])
        cron_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2563eb')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        self.story.append(cron_table)
        self.story.append(Spacer(1, 0.15*inch))
        
        # UI steps
        self.story.append(Paragraph("Cloud UI Configuration Steps", self.styles['CustomSubheading']))
        ui_steps = "For each flow: 1. Go to https://app.prefect.cloud 2. Deployments menu 3. Select flow 4. Schedules tab 5. Create Schedule 6. Enter Cron expression 7. Set timezone to Asia/Shanghai 8. Enable 9. Save"
        self.story.append(Paragraph(ui_steps, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_4_worker(self):
        """Add worker management section"""
        self.story.append(Paragraph("4. Worker Management", self.styles['CustomHeading']))
        
        self.story.append(Paragraph(
            "The Worker is a process that polls Prefect Cloud for scheduled flows and executes them on your computer.",
            self.styles['CustomBody']
        ))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Starting worker
        self.story.append(Paragraph("Starting the Worker", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Open PowerShell and run:",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "prefect worker start --pool Yichen_Test",
            self.styles['CodeStyle']
        ))
        self.story.append(Spacer(1, 0.1*inch))
        
        # What to expect
        self.story.append(Paragraph("Expected Output", self.styles['CustomSubheading']))
        output = "Worker started successfully<br/>Listening for flow runs...<br/>[Waiting for runs...]"
        self.story.append(Paragraph(output, self.styles['CodeStyle']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Important notes
        self.story.append(Paragraph("Important Notes", self.styles['CustomSubheading']))
        notes = "<b>Keep terminal open:</b> Worker must stay running<br/><b>Keep computer on:</b> During execution times<br/><b>Network required:</b> Must have internet<br/><b>API key valid:</b> Must be logged in"
        self.story.append(Paragraph(notes, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_5_operations(self):
        """Add daily operations section"""
        self.story.append(Paragraph("5. Daily Operations", self.styles['CustomHeading']))
        
        # Monitoring
        self.story.append(Paragraph("Monitoring Execution", self.styles['CustomSubheading']))
        monitoring = "In Cloud UI: Deployments tab shows all running flows. Select flow to view run history and logs. In Terminal: Worker output shows run acceptance and completion."
        self.story.append(Paragraph(monitoring, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Checking logs
        self.story.append(Paragraph("Viewing Execution Logs", self.styles['CustomSubheading']))
        logs = "Cloud UI Logs: Deployments menu then select flow and view logs. Local Logs: Check 6_logs/ directory."
        self.story.append(Paragraph(logs, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Manual triggers
        self.story.append(Paragraph("Manual Trigger (For Testing)", self.styles['CustomSubheading']))
        self.story.append(Paragraph(
            "Run a flow immediately without waiting for schedule:",
            self.styles['CustomBody']
        ))
        self.story.append(Paragraph(
            "prefect deployment run currency-acquisition",
            self.styles['CodeStyle']
        ))
        self.story.append(PageBreak())
        
    def add_section_6_automatic(self):
        """Add automatic execution section"""
        self.story.append(Paragraph("6. Automatic Execution Guide", self.styles['CustomHeading']))
        
        # How it works
        self.story.append(Paragraph("How Automatic Execution Works", self.styles['CustomSubheading']))
        how_it_works = "Prefect Cloud monitors all schedules 24/7. At scheduled time, creates Flow Run. Your Worker polls Cloud every 3 seconds. When new run appears, accepts and executes it. Uploads results back to Cloud. Key requirement: Computer must be online when execution time arrives."
        self.story.append(Paragraph(how_it_works, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Recommended setup
        self.story.append(Paragraph("Recommended Setup", self.styles['CustomSubheading']))
        setup = "Option 1: Leave computer running 24/7. Option 2: Set reminders for trigger dates. Turn on 5 min before execution. Start Worker. Keep running until 10:15. Turn off after."
        self.story.append(Paragraph(setup, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_7_troubleshooting(self):
        """Add troubleshooting section"""
        self.story.append(Paragraph("7. Troubleshooting", self.styles['CustomHeading']))
        
        # Problem 1
        self.story.append(Paragraph("Problem: Run is Still QUEUED", self.styles['CustomSubheading']))
        p1 = "Cause: Worker offline during execution. Solution: Start Worker now and manually trigger the flow."
        self.story.append(Paragraph(p1, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.1*inch))
        
        # Problem 2
        self.story.append(Paragraph("Problem: CSV File Not Created", self.styles['CustomSubheading']))
        p2 = "Check Cloud UI logs for errors. Verify API connectivity. Check directory permissions."
        self.story.append(Paragraph(p2, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.1*inch))
        
        # Problem 3
        self.story.append(Paragraph("Problem: Worker Crashes", self.styles['CustomSubheading']))
        p3 = "Check network connectivity. Re-login to Cloud. Restart Worker."
        self.story.append(Paragraph(p3, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_8_api(self):
        """Add API documentation section"""
        self.story.append(Paragraph("8. API Documentation", self.styles['CustomHeading']))
        
        # Exchange rate fetcher
        self.story.append(Paragraph("Exchange Rate Fetcher", self.styles['CustomSubheading']))
        fetcher = "Fetches FX rates from IMF SDMX API. Supports 118 countries and 77 currencies. Data is fresh daily. REST Countries API cached for 5 minutes."
        self.story.append(Paragraph(fetcher, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Data output
        self.story.append(Paragraph("Data Output Format", self.styles['CustomSubheading']))
        output = "File: exchange_rates_YYYY_MM.csv. Location: data/ directory. Format: UTF-8 CSV. Columns: Country, Currency, Date, Exchange_Rate, Base_Currency, Timestamp"
        self.story.append(Paragraph(output, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_9_support(self):
        """Add support section"""
        self.story.append(Paragraph("9. Support & Contacts", self.styles['CustomHeading']))
        
        # Resources
        self.story.append(Paragraph("Available Resources", self.styles['CustomSubheading']))
        resources = "Internal Documentation: README_EN.md, QUICK_START_EN.md, SCHEDULE_SETUP_GUIDE_EN.md. External: Prefect Cloud UI at https://app.prefect.cloud, Prefect Docs at https://docs.prefect.io"
        self.story.append(Paragraph(resources, self.styles['CustomBody']))
        self.story.append(Spacer(1, 0.15*inch))
        
        # Escalation
        self.story.append(Paragraph("When to Get Help", self.styles['CustomSubheading']))
        escalation = "For Code Issues: Check error logs in Cloud UI. For Network Issues: Verify connectivity. For API Issues: Check API status. For Setup: Review QUICK_START_EN.md"
        self.story.append(Paragraph(escalation, self.styles['CustomBody']))
        self.story.append(PageBreak())
        
    def add_section_10_appendix(self):
        """Add appendix section"""
        self.story.append(Paragraph("10. Appendix - Reference", self.styles['CustomHeading']))
        
        # Common commands
        self.story.append(Paragraph("Common Commands", self.styles['CustomSubheading']))
        commands = [
            ('prefect cloud login', 'Login to Prefect Cloud'),
            ('prefect deploy', 'Deploy all flows'),
            ('prefect deployment ls', 'List all deployments'),
            ('prefect worker start --pool Yichen_Test', 'Start Worker'),
            ('prefect deployment run [NAME]', 'Manually trigger flow'),
        ]
        
        cmd_data = [['Command', 'Purpose']]
        for cmd, purpose in commands:
            cmd_data.append([cmd, purpose])
        
        cmd_table = Table(cmd_data, colWidths=[2.5*inch, 1.8*inch])
        cmd_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2563eb')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        self.story.append(cmd_table)
        self.story.append(Spacer(1, 0.2*inch))
        
        # Timeline
        self.story.append(Paragraph("Execution Timeline", self.styles['CustomSubheading']))
        timeline = "Each trigger date: 09:00 currency-acquisition starts. 09:30 prepare-batch starts. 10:00 process-batch starts. 10:10 all complete."
        self.story.append(Paragraph(timeline, self.styles['CustomBody']))
        
    def build(self):
        """Build the PDF"""
        self.add_title_page()
        self.add_table_of_contents()
        self.add_section_1_overview()
        self.add_section_2_setup()
        self.add_section_3_scheduling()
        self.add_section_4_worker()
        self.add_section_5_operations()
        self.add_section_6_automatic()
        self.add_section_7_troubleshooting()
        self.add_section_8_api()
        self.add_section_9_support()
        self.add_section_10_appendix()
        
        # Build PDF
        self.doc.build(self.story)
        print(f"OK PDF generated: {self.output_filename}")
        print(f"Location: {os.path.abspath(self.output_filename)}")
        file_size = os.path.getsize(self.output_filename) / 1024
        print(f"File size: {file_size:.1f} KB")
        

def main():
    """Main function"""
    print("=" * 60)
    print("Prefect Operation Manual PDF Generator")
    print("=" * 60)
    print()
    
    # Generate PDF
    generator = PDFGenerator("Prefect_Operation_Manual.pdf")
    generator.build()
    
    print()
    print("=" * 60)
    print("Done! Your operation manual is ready.")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Open the PDF to review")
    print("2. Print for physical reference if needed")
    print("3. Share with team members")
    print()


if __name__ == "__main__":
    main()
