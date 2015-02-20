module Arel
  module Visitors
    class Oracle < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_SelectStatement o, a
        o = order_hacks(o, a)
        o = where_hacks(o, a)

        # if need to select first records without ORDER BY and GROUP BY and without DISTINCT
        # then can use simple ROWNUM in WHERE clause
        if o.limit && o.orders.empty? && !o.offset && o.cores.first.set_quantifier.class.to_s !~ /Distinct/
          o.cores.last.wheres.push Nodes::LessThanOrEqual.new(
            Nodes::SqlLiteral.new('ROWNUM'), o.limit.expr
          )
          return super
        end

        if o.limit && o.offset
          o        = o.dup
          limit    = o.limit.expr.to_i
          offset   = o.offset
          o.offset = nil
          sql = super(o, a)
          return <<-eosql
              SELECT * FROM (
                SELECT raw_sql_.*, rownum raw_rnum_
                FROM (#{sql}) raw_sql_
                WHERE rownum <= #{offset.expr.to_i + limit}
              )
              WHERE #{visit offset, a}
          eosql
        end

        if o.limit
          o       = o.dup
          limit   = o.limit.expr
          return "SELECT * FROM (#{super(o, a)}) WHERE ROWNUM <= #{visit limit, a}"
        end

        if o.offset
          o        = o.dup
          offset   = o.offset
          o.offset = nil
          sql = super(o, a)
          return <<-eosql
              SELECT * FROM (
                SELECT raw_sql_.*, rownum raw_rnum_
                FROM (#{sql}) raw_sql_
              )
              WHERE #{visit offset, a}
          eosql
        end

        super
      end

      def visit_Arel_Nodes_Limit o, a
      end

      def visit_Arel_Nodes_Offset o, a
        "raw_rnum_ > #{visit o.expr, a}"
      end

      def visit_Arel_Nodes_Except o, a
        "( #{visit o.left, a} MINUS #{visit o.right, a} )"
      end

      def visit_Arel_Nodes_UpdateStatement o, a
        # Oracle does not allow ORDER BY/LIMIT in UPDATEs.
        if o.orders.any? && o.limit.nil?
          # However, there is no harm in silently eating the ORDER BY clause if no LIMIT has been provided,
          # otherwise let the user deal with the error
          o = o.dup
          o.orders = []
        end

        super
      end

      ###
      # Hacks for the order clauses specific to Oracle
      def order_hacks o, a
        return o if o.orders.empty?
        return o unless o.cores.any? do |core|
          core.projections.any? do |projection|
            /FIRST_VALUE/ === projection
          end
        end
        # Previous version with join and split broke ORDER BY clause
        # if it contained functions with several arguments (separated by ',').
        #
        # orders   = o.orders.map { |x| visit x, a }.join(', ').split(',')
        orders   = o.orders.map do |x|
          string = visit x, a
          if string.include?(',')
            split_order_string(string)
          else
            string
          end
        end.flatten
        o.orders = []
        orders.each_with_index do |order, i|
          o.orders <<
            Nodes::SqlLiteral.new("alias_#{i}__#{' DESC' if /\bdesc$/i === order}")
        end
        o
      end

      ###
      # Hacks for the where clauses specific to Oracle
      def where_hacks o, a
        o.cores.each do |core|
          core.wheres.each do |where|
            where.each do |node|
              next unless node.is_a?(Arel::Nodes::And)
              node.children.map! do |child|
                split_in_node(child)
              end
            end
          end
        end
        o
      end

      MAX_IN_EXPRESSIONS = 1000

      # If the node is an "in" expression which has too many values
      # for Oracle to handle, then turn it into a union of smaller
      # "in" expressions.
      def split_in_node(node)
        return node unless node.is_a?(Arel::Nodes::In)
        return node unless node.right.size > MAX_IN_EXPRESSIONS
        exprs = node.right.each_slice(MAX_IN_EXPRESSIONS).map do |right_nodes|
          in_node = Arel::Nodes::In.new(node.left, right_nodes)
          in_node.to_sql
        end
        expr = exprs.join(" OR ")
        Arel::Nodes::Grouping.new(Arel::Nodes::SqlLiteral.new(expr))
      end

      # Split string by commas but count opening and closing brackets
      # and ignore commas inside brackets.
      def split_order_string(string)
        array = []
        i = 0
        string.split(',').each do |part|
          if array[i]
            array[i] << ',' << part
          else
            # to ensure that array[i] will be String and not Arel::Nodes::SqlLiteral
            array[i] = '' << part
          end
          i += 1 if array[i].count('(') == array[i].count(')')
        end
        array
      end

    end
  end
end
