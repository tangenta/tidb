package parser

func (p *Parser) InProcedure() {
	p.inProcedure = true
}

func (p *Parser) OutProcedure() {
	p.inProcedure = false
}
